package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.*
import org.bson.types.ObjectId

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection}

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

  private val container = new MongoDBContainer("mongo:7")

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
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]], _ => fs2.Stream.empty)
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
  }
