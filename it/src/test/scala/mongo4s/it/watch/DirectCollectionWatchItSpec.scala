package mongo4s.it.watch

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}
import com.mongodb.client.model.changestream.OperationType

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.changestream.WatchOptions
import mongo4s.{Field, MongoClient, RsBridge}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class DirectCollectionWatchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import WatchFixture.Person

  type S[A] = CatsStream[IO][A]

  "watch on a getDirectCollection, decoding through WireCodec rather than BsonDocument" should {

    "deliver insert, update and delete with the current fullDocument" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("direct_watch")
          collection <- database.getDirectCollection[Person]("events")
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

    "surface an undecodable event as a Left through watchAttempting" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("direct_watch")
          collection <- database.getDirectCollection[Person]("attempting")
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
