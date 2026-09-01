package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.operations.{Update, WriteCommand}
import mongo4s.{Field, MongoClient, MongoCollection}

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class UnacknowledgedWriteItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val nameField = Field.of[Person, String](_.name)

  private def collection(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](s"${container.getConnectionString}/?w=0")
      database   <- client.getDatabase("unacknowledged_it")
      collection <- database.getCollection[Person](name)
    yield collection

  "a client connected with w:0" should {
    "report an empty InsertOneResult instead of failing" in {
      collection("insert_one")
        .flatMap(_.insertOne(Person("bob", 30)))
        .asserting(_.insertedId shouldBe None)
    }

    "report an empty InsertManyResult instead of failing" in {
      collection("insert_many")
        .flatMap(_.insertMany(List(Person("bob", 30), Person("alice", 25))))
        .asserting(_.insertedIds shouldBe Nil)
    }

    "report an empty UpdateResult instead of failing" in {
      collection("update_one")
        .flatMap(_.updateOne(nameField.equalTo("bob"), Update.set(nameField, "robert")))
        .asserting(_.wasApplied shouldBe false)
    }

    "report an empty DeleteResult instead of failing" in {
      collection("delete_one")
        .flatMap(_.deleteOne(nameField.equalTo("bob")))
        .asserting(_.deletedAny shouldBe false)
    }

    "report an empty BulkWriteResult instead of failing" in {
      collection("bulk")
        .flatMap(_.bulkWrite(Seq(WriteCommand.InsertOne(Person("bob", 30))), ordered = true))
        .asserting(_.insertedCount shouldBe 0L)
    }
  }
