package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import cats.syntax.all.*
import org.bson.*
import org.bson.types.ObjectId

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection, MongoError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object FakeFidelityParityItSpec:

  def strings(values: String*): BsonArray =
    BsonArray(values.toList.map(v => BsonString(v): BsonValue).asJava)

  def items(values: (String, Int)*): BsonArray =
    BsonArray(values.toList.map((n, q) => BsonDocument("name", BsonString(n)).append("qty", BsonInt32(q)): BsonValue).asJava)

final class FakeFidelityParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import FakeFidelityParityItSpec.*

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private val nameF  = Field.stored[BsonDocument, String]("name")
  private val ageF   = Field.stored[BsonDocument, Int]("age")
  private val priceF = Field.stored[BsonDocument, Double]("price")
  private val tagsF  = Field.stored[BsonDocument, List[String]]("tags")
  private val nickF  = Field.stored[BsonDocument, String]("nick")
  private val seenF  = Field.stored[BsonDocument, java.time.Instant]("seen")
  private val itemF  = Field.stored[BsonDocument, List[BsonValue]]("items") / "name"

  private def real(name: String, seed: List[BsonDocument]): IO[Coll] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_fidelity_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertMany(seed.map(_.clone()))
    yield collection

  private def fake(seed: List[BsonDocument]): IO[Coll] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])
    collection.insertMany(seed.map(_.clone())).as(collection)

  private def parity[A](name: String, seed: List[BsonDocument])(read: Coll => IO[A]): IO[(A, A)] =
    for
      server <- real(name, seed).flatMap(read)
      double <- fake(seed).flatMap(read)
    yield (server, double)

  private def namesOf(documents: List[BsonDocument]): List[String] =
    documents.map(_.getString("name").getValue).sorted

  private val people = List(
    BsonDocument("name", BsonString("foobar")).append("tags", strings("a", "b")).append("items", items(("pen", 2))),
    BsonDocument("name", BsonString("other")).append("tags", strings("c")).append("items", items(("cup", 1))),
  )

  private def agree[A](name: String, seed: List[BsonDocument])(read: Coll => IO[A]) =
    parity(name, seed)(read).timeout(30.seconds).asserting((server, double) => double shouldBe server)

  "the fake and the server" should {

    "agree on matching an element of an array" in {
      agree("contains", people)(_.find(tagsF.contains("a")).all.map(namesOf))
    }

    "agree on $ne over an array" in {
      agree("ne_array", people)(_.find(Filter.Ne(tagsF.path, BsonString("a"))).all.map(namesOf))
    }

    "agree on $in over an array" in {
      agree("in_array", people)(_.find(Filter.In(tagsF.path, List(BsonString("b"), BsonString("z")))).all.map(namesOf))
    }

    "agree that $all over an empty list matches nothing" in {
      agree("all_empty", people)(_.find(Filter.All(tagsF.path, Nil)).all.map(namesOf))
    }

    "agree on a dotted path through an array of documents" in {
      agree("dotted", people)(_.find(Filter.Eq(itemF.path, BsonString("pen"))).all.map(namesOf))
    }

    "agree on a regex that searches rather than anchors" in {
      agree("regex", people)(_.find(nameF.regex("oob")).all.map(namesOf))
    }

    "agree on a case-insensitive regex" in {
      agree("regex_i", people)(_.find(Filter.Regex(nameF.path, "FOO", "i")).all.map(namesOf))
    }

    "agree on $eq null against a missing field" in {
      val seed = List(
        BsonDocument("name", BsonString("present")).append("nick", BsonNull.VALUE),
        BsonDocument("name", BsonString("absent")),
      )
      agree("eq_null", seed)(_.find(Filter.Eq(nickF.path, BsonNull.VALUE)).all.map(namesOf))
    }

    "agree on $ne null against a missing field" in {
      val seed = List(
        BsonDocument("name", BsonString("present")).append("nick", BsonNull.VALUE),
        BsonDocument("name", BsonString("absent")),
        BsonDocument("name", BsonString("set")).append("nick", BsonString("x")),
      )
      agree("ne_null", seed)(_.find(Filter.Ne(nickF.path, BsonNull.VALUE)).all.map(namesOf))
    }

    "agree on the width $inc leaves behind" in {
      val seed = List(BsonDocument("name", BsonString("a")).append("age", BsonInt32(30)))
      agree("inc_width", seed) { collection =>
        for
          _     <- collection.updateOne(nameF.equalTo("a"), Update.inc(ageF, 1))
          found <- collection.find(ageF.equalTo(31)).all
        yield (found.size, found.headOption.map(_.get("age")))
      }
    }

    "agree on a fractional $inc" in {
      val seed = List(BsonDocument("name", BsonString("a")).append("price", BsonDouble(10.0)))
      agree("inc_double", seed) { collection =>
        collection.updateOne(nameF.equalTo("a"), Update.inc(priceF, 0.5)) *>
          collection.find().all.map(_.head.get("price"))
      }
    }

    "agree on $count when nothing reaches it" in {
      agree("count_empty", people) {
        _.aggregate[BsonDocument](Seq(Stage.matching(Filter.none[BsonDocument]), Stage.count("n"))).all
      }
    }

    "agree on which document findOneAndDelete takes under a sort" in {
      val seed = List(
        BsonDocument("name", BsonString("a")).append("age", BsonInt32(1)),
        BsonDocument("name", BsonString("b")).append("age", BsonInt32(2)),
      )
      agree("find_delete_sort", seed) {
        _.findOneAndDelete(Filter.all[BsonDocument], FindOneAndDeleteOptions.default[BsonDocument].withSort(Sort.desc(ageF)))
          .map(_.map(_.getString("name").getValue))
      }
    }

    "agree on ordering by a date" in {
      val seed = List(
        BsonDocument("name", BsonString("early")).append("seen", BsonDateTime(1000)),
        BsonDocument("name", BsonString("late")).append("seen", BsonDateTime(2000)),
      )
      agree("sort_date", seed)(_.find().sort(Sort.desc(seenF)).all.map(_.map(_.getString("name").getValue)))
    }

    "agree on ordering by a boolean" in {
      val seed = List(
        BsonDocument("name", BsonString("no")).append("ok", BsonBoolean(false)),
        BsonDocument("name", BsonString("yes")).append("ok", BsonBoolean(true)),
      )
      agree("sort_bool", seed) {
        _.find().sort(Sort.desc(Field.stored[BsonDocument, Boolean]("ok"))).all.map(_.map(_.getString("name").getValue))
      }
    }

    "agree on ordering by an ObjectId" in {
      val first  = ObjectId.get()
      val second = ObjectId.get()
      val seed   = List(
        BsonDocument("name", BsonString("second")).append("oid", BsonObjectId(second)),
        BsonDocument("name", BsonString("first")).append("oid", BsonObjectId(first)),
      )
      agree("sort_oid", seed) {
        _.find().sort(Sort.asc(Field.stored[BsonDocument, ObjectId]("oid"))).all.map(_.map(_.getString("name").getValue))
      }
    }

    "agree on what a replaceOne upsert reports when it inserts" in {
      val seed = List(BsonDocument("name", BsonString("present")))
      agree("upsert_insert", seed) { collection =>
        for
          result <- collection.replaceOne(nameF.equalTo("absent"), BsonDocument("name", BsonString("absent")), ReplaceOptions.upsert)
          stored <- collection.find(nameF.equalTo("absent")).all
        yield (
          result.matchedCount,
          result.modifiedCount,
          result.wasUpserted,
          result.wasApplied,
          result.upsertedId.map(_.getBsonType),
          result.upsertedId == stored.headOption.flatMap(document => Option(document.get("_id"))),
        )
      }
    }

    "agree on what a replaceOne upsert reports when it matches" in {
      val seed = List(BsonDocument("name", BsonString("present")))
      agree("upsert_match", seed) {
        _.replaceOne(nameF.equalTo("present"), BsonDocument("name", BsonString("present")), ReplaceOptions.upsert)
          .map(result => (result.matchedCount, result.modifiedCount, result.wasUpserted, result.upsertedId))
      }
    }

    "agree on what a bulk replaceOne upsert reports" in {
      val seed = List(BsonDocument("name", BsonString("present")))
      agree("upsert_bulk", seed) { collection =>
        val commands = List(
          WriteCommand.replaceOne(nameF.equalTo("present"), BsonDocument("name", BsonString("present")), ReplaceOptions.upsert),
          WriteCommand.replaceOne(nameF.equalTo("first"), BsonDocument("name", BsonString("first")), ReplaceOptions.upsert),
          WriteCommand.replaceOne(nameF.equalTo("second"), BsonDocument("name", BsonString("second")), ReplaceOptions.upsert),
        )

        for
          result <- collection.bulkWrite(commands)
          stored <- collection.find().all
        yield
          val idsByName = stored.flatMap(d => Option(d.get("_id")).map(d.getString("name").getValue -> _)).toMap

          (
            result.matchedCount,
            result.modifiedCount,
            result.upsertedIds.keySet,
            result.upsertedIds.values.map(_.getBsonType).toSet,
            result.upsertedIds == Map(1 -> idsByName("first"), 2 -> idsByName("second")),
          )
      }
    }

    "agree that a bulk insertOne stamps an _id" in {
      agree("bulk_insert_id", Nil) { collection =>
        for
          _      <- collection.bulkWrite(List(WriteCommand.InsertOne(BsonDocument("name", BsonString("fresh")))))
          stored <- collection.find().all
        yield stored.map(document => Option(document.get("_id")).map(_.getBsonType))
      }
    }

    "agree on ordering Longs past Double's precision, and NaN among numbers" in {
      val seed = List(
        BsonDocument("name", BsonString("bigger")).append("n", BsonInt64((1L << 53) + 1)),
        BsonDocument("name", BsonString("smaller")).append("n", BsonInt64(1L << 53)),
        BsonDocument("name", BsonString("nan")).append("n", BsonDouble(Double.NaN)),
      )
      agree("long_order", seed)(_.find().sort(Sort.asc(Field.stored[BsonDocument, Long]("n"))).all.map(_.map(_.getString("name").getValue)))
    }

    "agree on matching a number stored at another width" in {
      val seed = List(BsonDocument("name", BsonString("five")).append("n", BsonInt64(5)))
      agree("width_match", seed) { collection =>
        for
          asInt    <- collection.find(Filter.Eq(Field.stored[BsonDocument, Int]("n").path, BsonInt32(5))).all
          asDouble <- collection.find(Filter.Eq(Field.stored[BsonDocument, Double]("n").path, BsonDouble(5.0))).all
        yield (namesOf(asInt), namesOf(asDouble))
      }
    }

    "agree on what a unique index refuses, on insert and on update, and on the name it is given" in {
      val emailF = Field.stored[BsonDocument, String]("email")
      val seed   = List(keyed("a").head.append("email", BsonString("x@y")), keyed("b").head.append("email", BsonString("z@y")))

      agree("unique_index", seed) { collection =>
        for
          name     <- collection.createIndex(Index.ascending(emailF).withUnique)
          inserted <- collection.insertOne(keyed("c").head.append("email", BsonString("x@y"))).attempt
          updated  <- collection.updateOne(emailF.equalTo("z@y"), Update.set(emailF, "x@y")).attempt
          stored   <- collection.count()
        yield (name, inserted.left.toOption.map(_.getClass.getSimpleName), updated.left.toOption.map(_.getClass.getSimpleName), stored)
      }
    }

    "agree that a sparse unique index lets documents without the field through" in {
      agree("sparse_unique", keyed("a")) { collection =>
        for
          _      <- collection.createIndex(Index.ascending(Field.stored[BsonDocument, String]("email")).withUnique.withSparse)
          _      <- collection.insertOne(keyed("b").head)
          stored <- collection.count()
        yield stored
      }
    }

    "agree on refusing to build a unique index over documents that already break it" in {
      val seed = List(keyed("a").head.append("email", BsonString("x@y")), keyed("b").head.append("email", BsonString("x@y")))

      agree("unique_over_duplicates", seed) {
        _.createIndex(Index.ascending(Field.stored[BsonDocument, String]("email")).withUnique).attempt.map(_.left.toOption.map(_.getClass.getSimpleName))
      }
    }

    "agree that a replacement without an _id keeps the one it replaced" in {
      agree("replace_keeps_id", keyed("a").map(_.append("name", BsonString("old")))) { collection =>
        for
          _      <- collection.replaceOne(nameF.equalTo("old"), BsonDocument("name", BsonString("new")))
          stored <- collection.find().all
        yield stored
      }
    }

    "agree on refusing a replacement that changes the _id" in {
      agree("replace_changes_id", keyed("a").map(_.append("name", BsonString("old")))) { collection =>
        collection.replaceOne(nameF.equalTo("old"), keyed("b").head.append("name", BsonString("new"))).attempt.map {
          case Left(failed: MongoError) => Some(failed.code)
          case _                        => None
        }
      }
    }

    "agree that an update changing nothing matched but did not modify" in {
      agree("noop_update", keyed("a").map(_.append("name", BsonString("same")))) { collection =>
        collection.updateOne(nameF.equalTo("same"), Update.set(nameF, "same")).map(result => (result.matchedCount, result.modifiedCount))
      }
    }

    "agree on how an insertMany reports a duplicate _id inside it, and on what it kept" in {
      agree("insert_many_duplicate", Nil) { collection =>
        for
          failed <- collection.insertMany(keyed("a", "a", "b")).attempt
          stored <- collection.count()
        yield (bulkOutcome(failed), stored)
      }
    }

    "agree on which document a sorted updateOne and replaceOne change" in {
      val seed = List(("a", 3), ("b", 1), ("c", 2)).map((id, age) => BsonDocument("_id", BsonString(id)).append("age", BsonInt32(age)))

      agree("sorted_single_writes", seed) { collection =>
        for
          _        <- collection.updateOne(Filter.all, Update.set(nameF, "updated"), UpdateOptions.default.withSort(Sort.asc(ageF)))
          _        <- collection.replaceOne(Filter.all, BsonDocument("name", BsonString("replaced")), ReplaceOptions.default.withSort(Sort.desc(ageF)))
          updated  <- collection.find(nameF.equalTo("updated")).all
          replaced <- collection.find(nameF.equalTo("replaced")).all
        yield (updated.map(_.getString("_id").getValue), replaced.map(_.getString("_id").getValue))
      }
    }

    "agree on refusing a sort on updateMany, which the driver would drop without a word" in {
      val sorted = UpdateOptions.default.withSort(Sort.asc(ageF))

      def refusal(outcome: Either[Throwable, Any]): Option[(String, String)] =
        outcome.left.toOption.map(error => (error.getClass.getSimpleName, error.getMessage))

      agree("sorted_update_many", keyed("a")) { collection =>
        for
          many <- collection.updateMany(Filter.all, Update.set(nameF, "x"), sorted).attempt
          bulk <- collection.bulkWrite(List(WriteCommand.updateMany(Filter.all, Update.set(nameF, "x"), sorted))).attempt
        yield (refusal(many), refusal(bulk))
      }
    }

    "agree on an unordered insertMany carrying on past every duplicate _id" in {
      agree("insert_many_unordered", keyed("c")) { collection =>
        for
          failed <- collection.insertMany(keyed("a", "a", "b", "c", "d"), InsertManyOptions.unordered).attempt
          stored <- collection.count()
        yield (bulkOutcome(failed), stored)
      }
    }

    "agree on an ordered bulk write stopping at a duplicate _id" in {
      agree("bulk_ordered_duplicate", keyed("a")) { collection =>
        for
          failed <- collection.bulkWrite(keyed("b", "a", "c").map(WriteCommand.InsertOne(_))).attempt
          stored <- collection.count()
        yield (bulkOutcome(failed), stored)
      }
    }

    "agree on an unordered bulk write carrying on past a duplicate _id" in {
      agree("bulk_unordered_duplicate", keyed("a")) { collection =>
        for
          failed <- collection.bulkWrite(keyed("a", "b", "a").map(WriteCommand.InsertOne(_)), BulkWriteOptions.unordered).attempt
          stored <- collection.count()
        yield (bulkOutcome(failed), stored)
      }
    }

    "agree on building none of the indexes when one of them cannot be built" in {
      val emailF = Field.stored[BsonDocument, String]("email")
      val seed   = List(keyed("a").head.append("email", BsonString("x@y")), keyed("b").head.append("email", BsonString("x@y")))

      agree("indexes_all_or_nothing", seed) { collection =>
        for
          failed  <- collection.createIndexes(List(Index.ascending(nameF).named("fine"), Index.ascending(emailF).withUnique.named("clash"))).attempt
          indexes <- collection.listIndexes
        yield (failed.left.toOption.map(_.getClass.getSimpleName), indexNames(indexes))
      }
    }

    "agree on the names createIndexes reports, an index that already exists included" in {
      agree("indexes_existing", keyed("a")) { collection =>
        for
          _       <- collection.createIndex(Index.ascending(ageF).named("by_age"))
          names   <- collection.createIndexes(List(Index.ascending(ageF).named("by_age"), Index.ascending(nameF)))
          indexes <- collection.listIndexes
        yield (names, indexNames(indexes))
      }
    }

    "agree on refusing an index whose name is taken by a different one" in {
      agree("indexes_name_taken", keyed("a")) { collection =>
        for
          _      <- collection.createIndex(Index.ascending(ageF).named("taken"))
          failed <- collection.createIndex(Index.ascending(nameF).named("taken")).attempt
        yield codeOf(failed)
      }
    }

    "agree on refusing an index that already exists under another name" in {
      agree("indexes_other_name", keyed("a")) { collection =>
        for
          _      <- collection.createIndex(Index.ascending(ageF).named("first"))
          failed <- collection.createIndex(Index.ascending(ageF).named("second")).attempt
        yield codeOf(failed)
      }
    }

    "agree on refusing a unique index on _id alone" in {
      agree("indexes_unique_id", keyed("a")) { collection =>
        collection.createIndex(Index.unique(Field.stored[BsonDocument, String]("_id"))).attempt.map(codeOf)
      }
    }

    "agree on which filters a partial index accepts" in {
      val filters = List(
        ageF.gte(18),
        ageF.in(List(1, 2)) || nameF.exists,
        Filter.Type[BsonDocument](ageF.path, mongo4s.bson.BsonTypeName.Int),
        !ageF.gte(18),
        ageF.in(Nil),
        Filter.Ne[BsonDocument](ageF.path, BsonInt32(1)),
        nameF.regex("^a"),
        Filter.Exists[BsonDocument](nameF.path, false),
        tagsF.hasSize(1),
      )

      agree("indexes_partial", keyed("a")) { collection =>
        filters.zipWithIndex.traverse { (filter, i) =>
          collection.createIndex(Index.ascending(Field.stored[BsonDocument, Int](s"f$i")).where(filter)).attempt.map(codeOf)
        }
      }
    }

    "agree on how listIndexes describes each index" in {
      val indexes = List(
        Index.ascending(nameF).withUnique.withSparse.named("by_name"),
        Index.descending(ageF).where(ageF.gte(18)).named("adults"),
        Index.ascending(seenF).expiringAfter(2.hours).named("expiring"),
        Index.ascending(priceF).withHidden,
      )

      agree("indexes_described", keyed("a")) { collection =>
        collection.createIndexes(indexes) >> collection.listIndexes.map(_.sortBy(_.getString("name").getValue))
      }
    }

    "agree that dropIndexes keeps only the index on _id" in {
      agree("indexes_dropped", keyed("a")) { collection =>
        for
          _       <- collection.createIndexes(List(Index.ascending(nameF), Index.ascending(ageF)))
          _       <- collection.dropIndexes
          indexes <- collection.listIndexes
        yield indexNames(indexes)
      }
    }

    "agree on refusing to drop an index that does not exist, and the one on _id" in {
      agree("indexes_undroppable", keyed("a")) { collection =>
        for
          missing <- collection.dropIndex("nope").attempt
          id      <- collection.dropIndex("_id_").attempt
        yield (codeOf(missing), codeOf(id))
      }
    }
  }

  "the fake and the server, grouping documents with gaps in them" should {
    val score = Field.stored[BsonDocument, Int]("score")
    val attrs = Field.stored[BsonDocument, BsonDocument]("attrs")
    val group = Field.stored[BsonDocument, String]("g")
    val seq   = Field.stored[BsonDocument, Int]("seq")

    val seed = List(
      BsonDocument("seq", BsonInt32(1)).append("g", BsonString("a")).append("score", BsonInt32(3)).append("attrs", BsonDocument("x", BsonInt32(1))),
      BsonDocument("seq", BsonInt32(2)).append("g", BsonString("a")).append("attrs", BsonDocument("y", BsonInt32(2))),
      BsonDocument("seq", BsonInt32(3)).append("g", BsonString("a")).append("score", BsonNull.VALUE),
      BsonDocument("seq", BsonInt32(4)).append("g", BsonString("a")).append("score", BsonInt32(7)).append("attrs", BsonDocument("x", BsonInt32(3))),
    )

    def grouped(accumulators: (String, Accumulator[BsonDocument])*)(collection: Coll) =
      collection
        .aggregate[BsonDocument](Seq(Stage.sortBy(Sort.asc(seq)), Stage.groupBy(group)(accumulators*)))
        .all
        .map(_.map(document => { document.remove("_id"); document }))

    "agree on which accumulators keep a missing or null value and which drop it" in {
      agree("gaps_n_accumulators", seed) {
        grouped(
          "first"  -> Accumulator.firstN(score, 3),
          "last"   -> Accumulator.lastN(score, 2),
          "max"    -> Accumulator.maxN(score, 4),
          "min"    -> Accumulator.minN(score, 4),
          "top"    -> Accumulator.topN(Sort.asc(seq), score, 3),
          "bottom" -> Accumulator.bottom(Sort.asc(seq), score),
        )
      }
    }

    "agree on what $mergeObjects builds, skipping the documents that carry nothing" in {
      agree("gaps_merge_objects", seed)(grouped("merged" -> Accumulator.mergeObjects(attrs)))
    }

    "agree on refusing $mergeObjects over something that is not a document" in {
      agree("merge_objects_refused", seed) { collection =>
        grouped("merged" -> Accumulator.mergeObjects(score))(collection).attempt.map(codeOf)
      }
    }
  }

  "the fake and the server, filling gaps" should {
    val t    = Field.stored[BsonDocument, Int]("t")
    val v    = Field.stored[BsonDocument, Int]("v")
    val s    = Field.stored[BsonDocument, String]("s")
    val note = Field.stored[BsonDocument, String]("note")
    val g    = Field.stored[BsonDocument, String]("g")

    def row(id: String, group: String, at: Int, fields: (String, BsonValue)*): BsonDocument =
      fields.foldLeft(BsonDocument("_id", BsonString(id)).append("g", BsonString(group)).append("t", BsonInt32(at)))((acc, f) => acc.append(f._1, f._2))

    val seed = List(
      row("a1", "a", 1, "v" -> BsonInt32(10), "s"    -> BsonString("x")),
      row("a2", "a", 2, "s" -> BsonNull.VALUE),
      row("a3", "a", 4, "v" -> BsonInt32(40), "note" -> BsonString("kept")),
      row("a4", "a", 5),
      row("b1", "b", 1),
      row("b2", "b", 2, "v" -> BsonInt32(5), "s"     -> BsonString("y")),
    )

    def filledBy(stage: Stage[BsonDocument])(collection: Coll) =
      collection.aggregate[BsonDocument](Seq(stage, Stage.sortBy(Sort.asc(g).asc(t)))).all

    "agree on a fill by value, by the last value and by the line between neighbours, each within its partition" in {
      agree(
        "fill_methods",
        seed,
      )(filledBy(Stage.fill(Sort.asc(t), partitionBy = Seq(g.path))(FillOutput.linear(v), FillOutput.locf(s), FillOutput.value(note, "none"))))
    }

    "agree on a fill by value with no partition and no sort" in {
      agree("fill_value_only", seed)(filledBy(Stage.fill[BsonDocument]()(FillOutput.value(v, 0))))
    }
  }

  "the fake and the server, on bits" should {
    val bits = Field.stored[BsonDocument, Int]("bits")
    val wide = Field.stored[BsonDocument, Long]("wide")

    val seed = List(
      BsonDocument("_id", BsonString("five")).append("bits", BsonInt32(5)).append("wide", BsonInt64(1L << 40)),
      BsonDocument("_id", BsonString("six")).append("bits", BsonDouble(6.0)),
      BsonDocument("_id", BsonString("half")).append("bits", BsonDouble(6.5)),
      BsonDocument("_id", BsonString("text")).append("bits", BsonString("5")).append("label", BsonString("x")),
      BsonDocument("_id", BsonString("none")),
    )

    def ids(filter: Filter[BsonDocument])(collection: Coll) =
      collection.find(filter).all.map(_.map(_.getString("_id").getValue).sorted)

    "agree on which documents each $bits operator matches, whole doubles included" in {
      agree("bits_query", seed) { collection =>
        for
          allSet   <- ids(bits.bitsAllSet(4))(collection)
          anySet   <- ids(bits.bitsAnySet(2))(collection)
          allClear <- ids(bits.bitsAllClear(2))(collection)
          anyClear <- ids(bits.bitsAnyClear(5))(collection)
        yield (allSet, anySet, allClear, anyClear)
      }
    }

    "agree on what $bit writes, its width, and onto a missing field" in {
      agree("bit_update", seed) { collection =>
        for
          _      <- collection.updateOne(Filter.Eq(Field.stored[BsonDocument, String]("_id").path, BsonString("five")), Update.bitAnd(bits, 4))
          _      <- collection.updateOne(Filter.Eq(Field.stored[BsonDocument, String]("_id").path, BsonString("five")), Update.bitXor(wide, 1L))
          _      <- collection.updateOne(Filter.Eq(Field.stored[BsonDocument, String]("_id").path, BsonString("none")), Update.bitOr(bits, 3))
          stored <- collection.find().sort(Sort.asc(Field.stored[BsonDocument, String]("_id"))).all
        yield stored
      }
    }

    "agree on refusing $bit over a value that is not an integer" in {
      agree("bit_refused", seed) { collection =>
        collection.updateOne(Filter.Eq(Field.stored[BsonDocument, String]("_id").path, BsonString("text")), Update.bitOr(bits, 1)).attempt.map(codeOf)
      }
    }
  }

  private def indexNames(indexes: List[BsonDocument]): List[String] =
    indexes.map(_.getString("name").getValue).sorted

  private def codeOf(outcome: Either[Throwable, Any]): Option[Int] =
    outcome.left.toOption.collect { case failed: MongoError => failed.code }

  private def keyed(ids: String*): List[BsonDocument] = ids.toList.map(id => BsonDocument("_id", BsonString(id)))

  private def bulkOutcome(outcome: Either[Throwable, Any]): Option[(List[Int], Long)] =
    outcome match
      case Left(failed: MongoError.BulkWriteFailed) => Some((failed.failures.map(_.index), failed.result.insertedCount))
      case _                                        => None
