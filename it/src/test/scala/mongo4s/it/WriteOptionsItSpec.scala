package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import com.mongodb.client.model.Collation
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.WireCodec

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object WriteOptionsItSpec:
  final case class Note(id: String, body: String) derives WireCodec

final class WriteOptionsItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import WriteOptionsItSpec.Note

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val idField   = Field.of[Note, String](_.id)
  private val bodyField = Field.of[Note, String](_.body)

  private def seeded(name: String) =
    for
      client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database <- client.getDatabase("write_options_it")
      notes    <- database.getDirectCollection[Note](name)
      _        <- notes.insertMany(List(Note("1", "alpha"), Note("2", "BETA"), Note("3", "gamma")))
    yield notes

  "CountOptions" should {
    "apply skip and limit, which the server does rather than the client" in {
      val program =
        for
          notes   <- seeded("counted")
          all     <- notes.count()
          skipped <- notes.count(Filter.all, CountOptions.default.withSkip(1))
          capped  <- notes.count(Filter.all, CountOptions.default.withLimit(2))
        yield (all, skipped, capped)

      program.asserting { (all, skipped, capped) =>
        all shouldBe 3L
        skipped shouldBe 2L
        capped shouldBe 2L
      }
    }
  }

  "a collation" should {
    "make a delete case-insensitive, which it is not by default" in {
      val caseInsensitive = Collation.builder().locale("en").collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()

      val program =
        for
          notes     <- seeded("collated")
          untouched <- notes.deleteMany(bodyField.equalTo("beta"))
          removed   <- notes.deleteMany(bodyField.equalTo("beta"), DeleteOptions.default.withCollation(caseInsensitive))
          left      <- notes.find(Filter.all).all
        yield (untouched.deletedCount, removed.deletedCount, left.map(_.id).sorted)

      program.asserting { (untouched, removed, left) =>
        untouched shouldBe 0L
        removed shouldBe 1L
        left shouldBe List("1", "3")
      }
    }
  }

  "a comment" should {
    "reach the server on an update without changing the result" in {
      val program =
        for
          notes   <- seeded("commented")
          updated <- notes.updateOne(idField.equalTo("1"), Update.set(bodyField, "changed"), UpdateOptions.default.withComment("it-spec"))
          found   <- notes.find(idField.equalTo("1")).first
        yield (updated.matchedCount, found)

      program.asserting { (matched, found) =>
        matched shouldBe 1L
        found shouldBe Some(Note("1", "changed"))
      }
    }
  }

  "the findOneAnd* options" should {
    "carry a collation, so a case-insensitive match finds what a plain one misses" in {
      val caseInsensitive = Collation.builder().locale("en").collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()

      val program =
        for
          notes  <- seeded("find_one_collated")
          missed <- notes.findOneAndUpdate(bodyField.equalTo("beta"), Update.set(bodyField, "changed"))
          found  <- notes.findOneAndUpdate(
                      bodyField.equalTo("beta"),
                      Update.set(bodyField, "changed"),
                      FindOneAndUpdateOptions.default[Note].withCollation(caseInsensitive),
                    )
        yield (missed, found)

      program.asserting { (missed, found) =>
        missed shouldBe None
        found shouldBe Some(Note("2", "changed"))
      }
    }

    "carry a hint to the server, which refuses one naming an index that does not exist" in {
      val program =
        for
          notes  <- seeded("find_one_hinted")
          failed <- notes.findOneAndDelete(idField.equalTo("1"), FindOneAndDeleteOptions.default[Note].withHint(BsonDocument("missing", BsonInt32(1)))).attempt
          kept   <- notes.count()
        yield (failed.isLeft, kept)

      program.asserting((failed, kept) => (failed, kept) shouldBe (true, 3L))
    }

    "carry bypassDocumentValidation, letting a replacement past the validator that refuses it otherwise" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("write_options_it")
          _        <- database.createCollection("validated", CreateCollectionOptions.default.withValidator(BsonDocument("body", BsonDocument("$ne", BsonString("forbidden")))))
          notes    <- database.getDirectCollection[Note]("validated")
          _        <- notes.insertOne(Note("1", "alpha"))
          refused  <- notes.findOneAndReplace(idField.equalTo("1"), Note("1", "forbidden")).attempt
          allowed  <- notes.findOneAndReplace(
                        idField.equalTo("1"),
                        Note("1", "forbidden"),
                        FindOneAndReplaceOptions.default[Note].withBypassDocumentValidation(true),
                      )
        yield (refused.isLeft, allowed)

      program.asserting((refused, allowed) => (refused, allowed) shouldBe (true, Some(Note("1", "forbidden"))))
    }

    "carry let variables to the $expr of every write that takes them" in {
      def bodyIs(variable: String): Filter[Note] =
        Filter.expr(BsonDocument("$eq", BsonArray(java.util.List.of(BsonString("$body"), BsonString(s"$$$$$variable")))))

      def vars(value: String): BsonDocument = BsonDocument("target", BsonString(value))

      val program =
        for
          notes    <- seeded("let_writes")
          updated  <- notes.updateOne(bodyIs("target"), Update.set(bodyField, "alpha2"), UpdateOptions.default.withLet(vars("alpha")))
          replaced <- notes.replaceOne(bodyIs("target"), Note("2", "beta2"), ReplaceOptions.default.withLet(vars("BETA")))
          found    <- notes.findOneAndUpdate(bodyIs("target"), Update.set(bodyField, "gamma2"), FindOneAndUpdateOptions.default[Note].withLet(vars("gamma")))
          swapped  <- notes.findOneAndReplace(bodyIs("target"), Note("1", "alpha3"), FindOneAndReplaceOptions.default[Note].withLet(vars("alpha2")))
          removed  <- notes.findOneAndDelete(bodyIs("target"), FindOneAndDeleteOptions.default[Note].withLet(vars("beta2")))
          bulk     <- notes.bulkWrite(List(WriteCommand.deleteOne(bodyIs("target"))), BulkWriteOptions.default.withLet(vars("gamma2")))
          deleted  <- notes.deleteMany(bodyIs("target"), DeleteOptions.default.withLet(vars("alpha3")))
          left     <- notes.count(Filter.all, CountOptions.default.withComment("it-spec"))
        yield (updated.modifiedCount, replaced.modifiedCount, found.map(_.body), swapped.map(_.body), removed.map(_.body), bulk.deletedCount, deleted.deletedCount, left)

      program.asserting(_ shouldBe (1L, 1L, Some("gamma2"), Some("alpha3"), Some("beta2"), 1L, 1L, 0L))
    }

    "carry bypassDocumentValidation and a comment on inserts, one and many" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("write_options_it")
          _        <- database.createCollection(
                        "validated_inserts",
                        CreateCollectionOptions.default.withValidator(BsonDocument("body", BsonDocument("$ne", BsonString("forbidden"))))
                      )
          notes    <- database.getDirectCollection[Note]("validated_inserts")
          refused  <- notes.insertOne(Note("1", "forbidden")).attempt
          _        <- notes.insertOne(Note("2", "forbidden"), InsertOneOptions.default.withBypassDocumentValidation(true).withComment("it-spec"))
          _        <- notes.insertMany(List(Note("3", "forbidden"), Note("4", "forbidden")), InsertManyOptions.default.withBypassDocumentValidation(true))
          kept     <- notes.count()
        yield (refused.isLeft, kept)

      program.asserting((refused, kept) => (refused, kept) shouldBe (true, 3L))
    }

    "carry a maxTime and a comment without changing the result" in {
      val program =
        for
          notes   <- seeded("find_one_limited")
          updated <- notes.findOneAndUpdate(
                       idField.equalTo("3"),
                       Update.set(bodyField, "changed"),
                       FindOneAndUpdateOptions.default[Note].withMaxTime(5.seconds).withComment("it-spec"),
                     )
        yield updated

      program.asserting(_ shouldBe Some(Note("3", "changed")))
    }
  }
