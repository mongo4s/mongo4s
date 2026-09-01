package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import com.mongodb.client.model.Collation

import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.WireCodec

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object WriteOptionsItSpec:
  final case class Note(id: String, body: String) derives WireCodec

final class WriteOptionsItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import WriteOptionsItSpec.Note

  private val container = new MongoDBContainer("mongo:7")

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
