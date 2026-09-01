package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}

import mongo4s.cats.CatsStream
import mongo4s.operations.Filter
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.WireCodec

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ConcernItSpec:
  final case class Note(id: String, body: String) derives WireCodec

final class ConcernItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ConcernItSpec.Note

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def database = MongoClient.fromConnectionString[IO, S](container.getConnectionString).flatMap(_.getDatabase("concern_it"))

  "withWriteConcern on a direct collection" should {
    "carry the concern to the driver and keep the derived WireCodec registered" in {
      val program =
        for
          db      <- database
          notes   <- db.getDirectCollection[Note]("notes")
          majority = notes.withWriteConcern(WriteConcern.MAJORITY)
          _       <- majority.insertOne(Note("1", "hello"))
          found   <- majority.find(Filter.all).all
        yield (majority.underlying.getWriteConcern, found)

      program.asserting { (concern, found) =>
        concern shouldBe WriteConcern.MAJORITY
        found shouldBe List(Note("1", "hello"))
      }
    }

    "leave the collection it was derived from untouched" in {
      val program =
        for
          db    <- database
          notes <- db.getDirectCollection[Note]("derived")
          _      = notes.withWriteConcern(WriteConcern.MAJORITY)
        yield notes.underlying.getWriteConcern

      program.asserting(_ should not be WriteConcern.MAJORITY)
    }
  }

  "withReadConcern and withReadPreference" should {
    "reach the driver on a database and the collections it opens" in {
      val program =
        for
          db    <- database
          local  = db.withReadConcern(ReadConcern.LOCAL).withReadPreference(ReadPreference.primaryPreferred)
          notes <- local.getDirectCollection[Note]("prefs")
        yield (local.underlying.getReadConcern, notes.underlying.getReadPreference)

      program.asserting { (concern, preference) =>
        concern shouldBe ReadConcern.LOCAL
        preference shouldBe ReadPreference.primaryPreferred
      }
    }
  }
