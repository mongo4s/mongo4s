package mongo4s.it.watch

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}
import com.mongodb.client.model.changestream.OperationType

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.Stage
import mongo4s.changestream.WatchOptions
import mongo4s.{Field, MongoClient, RsBridge}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class CollectionWatchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import WatchFixture.Person

  type S[A] = CatsStream[IO][A]

  private def updatesOnly: WatchOptions[Person] =
    WatchOptions.default[Person].withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("update"))))))

  "MongoCollection.watch" should {

    "report operationType and, thanks to UPDATE_LOOKUP, the current fullDocument on inserts and updates" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("collection_watch")
          collection <- database.getCollection[Person]("events")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- collection.watch(WatchOptions.default[Person].startingAt(now)).take(3).compile.toList.start

          _ <- collection.insertOne(Person("bob", 30))
          _ <- collection.updateOne(Field.of[Person, String](_.name).equalTo("bob"), Field.of[Person, Int](_.age).set(31))

          _ <- IO.sleep(500.millis)

          _      <- collection.deleteOne(Field.of[Person, String](_.name).equalTo("bob"))
          events <- fiber.joinWithNever
          _      <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE)
        events(0).fullDocument shouldBe Some(Person("bob", 30))
        events(1).fullDocument shouldBe Some(Person("bob", 31))
        events(2).fullDocument shouldBe None
      }
    }

    "carry documentKey, resumeToken and clusterTime on every event" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("collection_watch")
          collection <- database.getCollection[Person]("metadata")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- collection.watch(WatchOptions.default[Person].startingAt(now)).take(1).compile.toList.start
          _          <- collection.insertOne(Person("bob", 30))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        val event = events.head
        event.documentKey.map(_.containsKey("_id")) shouldBe Some(true)
        event.resumeToken.isEmpty shouldBe false
        event.clusterTime.isDefined shouldBe true
        event.updateDescription shouldBe None
      }
    }

    "report updateDescription on an update, naming the fields that changed" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("collection_watch")
          collection <- database.getCollection[Person]("update_description")
          _          <- collection.insertOne(Person("bob", 30))
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- collection.watch(updatesOnly.startingAt(now)).take(1).compile.toList.start
          _          <- collection.updateOne(Field.of[Person, String](_.name).equalTo("bob"), Field.of[Person, Int](_.age).set(31))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        val description = events.head.updateDescription
        description.isDefined shouldBe true
        description.get.getUpdatedFields.containsKey("age") shouldBe true
      }
    }

    "filter events through a raw pipeline matching against the change-event envelope" in {
      val insertOnly =
        WatchOptions.default[Person].withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))))

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("collection_watch")
          collection <- database.getCollection[Person]("filtered")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- collection.watch(insertOnly.startingAt(now)).take(1).compile.toList.start

          _      <- collection.insertOne(Person("alice", 25))
          _      <- collection.updateOne(Field.of[Person, String](_.name).equalTo("alice"), Field.of[Person, Int](_.age).set(26))
          events <- fiber.joinWithNever
          _      <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT)
      }
    }

    "surface an undecodable event as a Left rather than killing the stream" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("collection_watch")
          collection <- database.getCollection[Person]("attempting")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))

          fiber <- collection.watchAttempting(WatchOptions.default[Person].startingAt(now)).take(2).compile.toList.start

          _      <- collection.insertOne(Person("bob", 30))
          _      <- RsBridge[IO, S].one(collection.underlying.insertOne(BsonDocument().append("name", BsonString("broken"))))
          events <- fiber.joinWithNever

          _ <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.collect { case Right(event) => event }.flatMap(_.fullDocument) shouldBe List(Person("bob", 30))
        events.count(_.isLeft) shouldBe 1
      }
    }
  }
