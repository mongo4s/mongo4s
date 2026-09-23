package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonArray, BsonDocument, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.WireCodec

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object PipelineUpdateItSpec:
  final case class Note(id: String, body: String) derives WireCodec

final class PipelineUpdateItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import PipelineUpdateItSpec.Note

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val idField = Field.of[Note, String](_.id)

  private val exclaim = Update.pipeline(Stage.addFields[Note]("body" -> BsonDocument("$concat", BsonArray(java.util.List.of(BsonString("$body"), BsonString("!"))))))

  private def seeded(name: String) =
    for
      client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database <- client.getDatabase("pipeline_update_it")
      notes    <- database.getDirectCollection[Note](name)
      _        <- notes.insertMany(List(Note("1", "alpha"), Note("2", "beta")))
    yield notes

  "a pipeline update" should {

    "compute a field from the document's own value, on updateOne and updateMany" in {
      val program =
        for
          notes <- seeded("computed")
          _     <- notes.updateOne(idField.equalTo("1"), exclaim)
          _     <- notes.updateMany(Filter.all, exclaim)
          after <- notes.find().sort(Sort.asc(idField)).all
        yield after

      program.timeout(30.seconds).asserting(_ shouldBe List(Note("1", "alpha!!"), Note("2", "beta!")))
    }

    "work through findOneAndUpdate and a bulk write as well" in {
      val program =
        for
          notes   <- seeded("find_and_bulk")
          updated <- notes.findOneAndUpdate(idField.equalTo("1"), exclaim)
          _       <- notes.bulkWrite(List(WriteCommand.updateOne(idField.equalTo("2"), exclaim)))
          after   <- notes.find().sort(Sort.asc(idField)).all
        yield (updated, after)

      program.timeout(30.seconds).asserting { (updated, after) =>
        updated shouldBe Some(Note("1", "alpha!"))
        after shouldBe List(Note("1", "alpha!"), Note("2", "beta!"))
      }
    }
  }
