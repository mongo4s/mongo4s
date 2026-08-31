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
import mongo4s.{MongoClient, RsBridge}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class ScopeWatchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import WatchFixture.Person

  type S[A] = CatsStream[IO][A]

  private def onlyCollection[E](name: String): WatchOptions[E] =
    WatchOptions.default[E].withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("ns.coll", BsonString(name))))))

  "MongoDatabase.watch" should {

    "see events from every collection in its own database, as ChangeEvent[BsonDocument]" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database <- client.getDatabase("scope_watch_db")
          left     <- database.getCollection[Person]("left")
          right    <- database.getCollection[Person]("right")
          now      <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber    <- database.watch(WatchOptions.default[BsonDocument].startingAt(now)).take(2).compile.toList.start

          _      <- left.insertOne(Person("bob", 30))
          _      <- right.insertOne(Person("alice", 25))
          events <- fiber.joinWithNever
          _      <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT, OperationType.INSERT)
        events.flatMap(_.fullDocument).map(_.getString("name").getValue) shouldBe List("bob", "alice")
      }
    }

    "decode into a typed entity through watchAs" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("scope_watch_db_as")
          collection <- database.getCollection[Person]("people")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- database.watchAs[Person](WatchOptions.default[Person].startingAt(now)).take(1).compile.toList.start
          _          <- collection.insertOne(Person("bob", 30))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting(_.flatMap(_.fullDocument) shouldBe List(Person("bob", 30)))
    }

    "report an undecodable document through watchAsAttempting instead of failing the stream" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("scope_watch_db_attempting")
          collection <- database.getCollection[Person]("people")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- database.watchAsAttempting[Person](WatchOptions.default[Person].startingAt(now)).take(2).compile.toList.start

          _      <- collection.insertOne(Person("bob", 30))
          _      <- RsBridge[IO, S].one(collection.underlying.insertOne(BsonDocument().append("name", BsonString("broken"))))
          events <- fiber.joinWithNever
          _      <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.collect { case Right(event) => event }.flatMap(_.fullDocument) shouldBe List(Person("bob", 30))
        events.count(_.isLeft) shouldBe 1
      }
    }
  }

  "MongoClient.watch" should {

    "see events across the whole deployment, narrowed to one collection by pipeline" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("scope_watch_client")
          collection <- database.getCollection[Person]("deployment_wide")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- client.watch(onlyCollection[BsonDocument]("deployment_wide").startingAt(now)).take(1).compile.toList.start
          _          <- collection.insertOne(Person("bob", 30))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT)
        events.flatMap(_.fullDocument).map(_.getString("name").getValue) shouldBe List("bob")
      }
    }

    "decode into a typed entity through watchAs" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("scope_watch_client_as")
          collection <- database.getCollection[Person]("typed_deployment_wide")
          now        <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber      <- client.watchAs[Person](onlyCollection[Person]("typed_deployment_wide").startingAt(now)).take(1).compile.toList.start
          _          <- collection.insertOne(Person("bob", 30))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting(_.flatMap(_.fullDocument) shouldBe List(Person("bob", 30)))
    }
  }
