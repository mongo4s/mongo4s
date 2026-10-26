package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import cats.syntax.all.*
import org.bson.*

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection, MongoError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class FakeUpsertParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private val idF   = Field.stored[BsonDocument, String]("_id")
  private val nameF = Field.stored[BsonDocument, String]("name")
  private val ageF  = Field.stored[BsonDocument, Int]("age")
  private val nickF = Field.stored[BsonDocument, String]("nick")
  private val tagsF = Field.stored[BsonDocument, List[String]]("tags")
  private val cityF = Field.stored[BsonDocument, String]("profile.city")
  private val kindF = Field.stored[BsonDocument, String]("kind")

  private def real(name: String, seed: List[BsonDocument]): IO[Coll] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_upsert_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- if seed.isEmpty then collection.insertOne(BsonDocument("_id", BsonString("seed"))) else collection.insertMany(seed.map(_.clone()))
      _          <- if seed.isEmpty then collection.deleteMany(Filter.all) else IO.unit
    yield collection

  private def fake(seed: List[BsonDocument]): IO[Coll] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])
    collection.insertMany(seed.map(_.clone())).whenA(seed.nonEmpty).as(collection)

  private def agree[A](name: String, seed: List[BsonDocument] = Nil)(run: Coll => IO[A]) =
    (for
      server <- real(name, seed).flatMap(run)
      double <- fake(seed).flatMap(run)
    yield (server, double))
      .timeout(30.seconds)
      .asserting((server, double) => double shouldBe server)

  private def generatedIdsMarked(document: BsonDocument): BsonDocument =
    val copy = BsonDocument()
    document.entrySet.forEach { entry =>
      val value = if entry.getKey == "_id" && entry.getValue.isObjectId then BsonString("<generated>") else entry.getValue
      copy.append(entry.getKey, value): Unit
    }
    copy

  private def stored(collection: Coll): IO[List[BsonDocument]] =
    collection.find().all.map(_.map(generatedIdsMarked).sortBy(_.toJson))

  private def afterUpdate(filter: Filter[BsonDocument], update: Update[BsonDocument])(collection: Coll) =
    for
      result <- collection.updateOne(filter, update, UpdateOptions.upsert)
      docs   <- stored(collection)
    yield (result.matchedCount, result.modifiedCount, result.wasUpserted, docs)

  private def codeOf(outcome: Either[Throwable, Any]): Option[Int] =
    outcome.left.toOption.collect { case failed: MongoError => failed.code }

  "an upsert that inserts" should {

    "take the _id from an equality on it" in {
      agree("id_equality")(afterUpdate(idF.equalTo("k1"), Update.set(ageF, 3)))
    }

    "generate an _id when the filter does not pin one, and keep the equality fields" in {
      agree("generated_id")(afterUpdate(nameF.equalTo("ann"), Update.inc(ageF, 1)))
    }

    "put the _id first in the inserted document" in {
      agree("id_first") { collection =>
        for
          _    <- collection.updateOne(nameF.equalTo("ann"), Update.set(ageF, 1), UpdateOptions.upsert)
          _    <- collection.replaceOne(nameF.equalTo("bob"), BsonDocument("name", BsonString("bob")), ReplaceOptions.upsert)
          docs <- collection.find().all
        yield docs.map(_.keySet.asScala.toList)
      }
    }

    "build a nested document from an equality on a dotted path" in {
      agree("dotted")(afterUpdate(cityF.equalTo("Oslo"), Update.set(ageF, 1)))
    }

    "keep every equality of a conjunction and drop the range conditions" in {
      agree("conjunction")(afterUpdate(nameF.equalTo("ann") && kindF.equalTo("user") && ageF.gt(5), Update.set(nickF, "a")))
    }

    "seed from an $in with a single value" in {
      agree("in_single")(afterUpdate(nameF.in(List("ann")), Update.set(ageF, 1)))
    }

    "not seed from an $in with several values" in {
      agree("in_many")(afterUpdate(nameF.in(List("ann", "bob")), Update.set(ageF, 1)))
    }

    "seed from an $all with a single value" in {
      agree("all_single")(afterUpdate(tagsF.containsAll(List("x")), Update.set(ageF, 1)))
    }

    "refuse an $all with several values, which the server reads as several equalities on one path" in {
      agree("all_many")(_.updateOne(tagsF.containsAll(List("x", "y")), Update.set(ageF, 1), UpdateOptions.upsert).attempt.map(codeOf))
    }

    "seed from an $or with a single branch" in {
      agree("or_single")(afterUpdate(Filter.Or(List(nameF.equalTo("ann"))), Update.set(ageF, 1)))
    }

    "not seed from an $or with several branches" in {
      agree("or_many")(afterUpdate(nameF.equalTo("ann") || nameF.equalTo("bob"), Update.set(ageF, 1)))
    }

    "not seed from an $elemMatch" in {
      agree("elem_match")(afterUpdate(tagsF.elemMatch(Field.self[String].equalTo("x")), Update.set(ageF, 1)))
    }

    "seed an equality on null" in {
      agree("null_equality")(afterUpdate(Filter.Eq(nickF.path, BsonNull.VALUE), Update.set(ageF, 1)))
    }

    "seed an equality on a whole array" in {
      agree("array_equality")(afterUpdate(Filter.Eq(tagsF.path, BsonArray(List[BsonValue](BsonString("x"), BsonString("y")).asJava)), Update.set(ageF, 1)))
    }

    "let the update override a seeded field" in {
      agree("override")(afterUpdate(nameF.equalTo("ann"), Update.set(nameF, "bob")))
    }

    "increment a seeded field" in {
      agree("inc_seeded")(afterUpdate(ageF.equalTo(4), Update.inc(ageF, 1)))
    }

    "apply $setOnInsert" in {
      agree("set_on_insert")(afterUpdate(idF.equalTo("k1"), Update.setOnInsert(nameF, "ann").and(Update.set(ageF, 1))))
    }

    "refuse two equalities on one path" in {
      agree("same_path_twice") { collection =>
        collection.updateOne(Filter.And(List(nameF.equalTo("ann"), nameF.equalTo("bob"))), Update.set(ageF, 1), UpdateOptions.upsert).attempt.map(codeOf)
      }
    }

    "report a duplicate key against a unique index" in {
      agree("upsert_duplicate", List(BsonDocument("_id", BsonString("k0")).append("name", BsonString("ann")))) { collection =>
        for
          _       <- collection.createIndex(Index.ascending(nameF).withUnique)
          outcome <- collection.updateOne(idF.equalTo("k1"), Update.set(nameF, "ann"), UpdateOptions.upsert).attempt
        yield codeOf(outcome)
      }
    }
  }

  "an upsert that matches" should {

    "update the match and leave $setOnInsert out" in {
      agree("matches", List(BsonDocument("_id", BsonString("k1")).append("age", BsonInt32(1)))) {
        afterUpdate(idF.equalTo("k1"), Update.setOnInsert(nameF, "ann").and(Update.inc(ageF, 1)))
      }
    }
  }

  "updateMany with upsert" should {

    "insert one document when nothing matches" in {
      agree("many_insert") { collection =>
        for
          result <- collection.updateMany(kindF.equalTo("user"), Update.set(ageF, 1), UpdateOptions.upsert)
          docs   <- stored(collection)
        yield (result.matchedCount, result.wasUpserted, docs)
      }
    }

    "update every match and insert nothing when something matches" in {
      val seed = List(BsonDocument("_id", BsonString("a")).append("kind", BsonString("user")), BsonDocument("_id", BsonString("b")).append("kind", BsonString("user")))
      agree("many_match", seed) { collection =>
        for
          result <- collection.updateMany(kindF.equalTo("user"), Update.set(ageF, 1), UpdateOptions.upsert)
          docs   <- stored(collection)
        yield (result.matchedCount, result.modifiedCount, result.wasUpserted, docs)
      }
    }
  }

  "findOneAndUpdate with upsert" should {

    "return the inserted document when asked for the updated one" in {
      agree("find_update_after") { collection =>
        for
          returned <- collection.findOneAndUpdate(idF.equalTo("c"), Update.inc(ageF, 1), FindOneAndUpdateOptions.default[BsonDocument].withUpsert)
          docs     <- stored(collection)
        yield (returned, docs)
      }
    }

    "return nothing when asked for the previous one, and still insert" in {
      agree("find_update_before") { collection =>
        for
          returned <- collection.findOneAndUpdate(
                        nameF.equalTo("ann"),
                        Update.inc(ageF, 1),
                        FindOneAndUpdateOptions.default[BsonDocument].withUpsert.returningPrevious,
                      )
          docs     <- stored(collection)
        yield (returned, docs)
      }
    }

    "count up from the inserted document on the next call" in {
      agree("find_update_counter") { collection =>
        val next = collection.findOneAndUpdate(
          idF.equalTo("orders"),
          Update.inc(Field.stored[BsonDocument, Long]("value"), 1L),
          FindOneAndUpdateOptions.default[BsonDocument].withUpsert
        )
        for
          first  <- next
          second <- next
        yield (first, second)
      }
    }
  }

  "a replacement upsert" should {

    "take the _id from the filter and nothing else" in {
      agree("replace_id") { collection =>
        for
          result <- collection.replaceOne(idF.equalTo("k1") && kindF.equalTo("user"), BsonDocument("name", BsonString("ann")), ReplaceOptions.upsert)
          docs   <- stored(collection)
        yield (result.upsertedId, docs)
      }
    }

    "generate an _id when the filter does not pin one" in {
      agree("replace_generated") { collection =>
        for
          _    <- collection.replaceOne(nameF.equalTo("ann"), BsonDocument("age", BsonInt32(1)), ReplaceOptions.upsert)
          docs <- stored(collection)
        yield docs
      }
    }

    "insert through findOneAndReplace" in {
      agree("find_replace") { collection =>
        for
          returned <- collection.findOneAndReplace(idF.equalTo("k1"), BsonDocument("name", BsonString("ann")), FindOneAndReplaceOptions.default[BsonDocument].withUpsert)
          docs     <- stored(collection)
        yield (returned, docs)
      }
    }
  }

  "bulkWrite" should {

    "upsert through UpdateOne and UpdateMany and report where" in {
      agree("bulk_upsert", List(BsonDocument("_id", BsonString("a")).append("kind", BsonString("user")))) { collection =>
        for
          result <- collection.bulkWrite(
                      List(
                        WriteCommand.UpdateOne(idF.equalTo("a"), Update.set(ageF, 1), UpdateOptions.upsert),
                        WriteCommand.UpdateOne(idF.equalTo("b"), Update.set(ageF, 2), UpdateOptions.upsert),
                        WriteCommand.UpdateMany(kindF.equalTo("admin"), Update.set(ageF, 3), UpdateOptions.upsert),
                      )
                    )
          docs   <- stored(collection)
        yield (result.matchedCount, result.modifiedCount, result.upsertedIds.keySet, result.upsertedIds.get(1), docs)
      }
    }
  }
