package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer
import com.github.dockerjava.api.model.{ExposedPort, PortBinding, Ports}

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

final class WatchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:7").withCreateContainerCmdModifier { cmd =>
    cmd.getHostConfig
      .withPortBindings(
        PortBinding(
          Ports.Binding.bindPort(27017),
          ExposedPort(27017)
        )
      ): Unit
  }

  private var containerStarted = false

  override def beforeAll(): Unit =
    try
      container.start()
      containerStarted = true
    catch case _: Throwable => containerStarted = false

  private def requirePort(): Unit =
    if !containerStarted then cancel("host port 27017 is in use, so the replica set Testcontainers advertises as 127.0.0.1:27017 cannot be reached")

  private def connectionString: String = container.getConnectionString

  override def afterAll(): Unit = if containerStarted then container.stop()

  type S[A] = CatsStream[IO][A]

  "MongoCollection.watch" should {

    "report operationType and, thanks to UPDATE_LOOKUP, the current fullDocument on inserts and updates" in {
      requirePort()
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](connectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people")
          fiber      <- collection.watch().take(3).compile.toList.start

          _ <- IO.sleep(500.millis)

          _      <- collection.insertOne(Person("bob", 30))
          _      <- collection.updateOne(Field.of[Person, String](_.name).equalTo("bob"), Field.of[Person, Int](_.age).set(31))
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

    "filter events through a raw pipeline matching against the change-event envelope" in {
      val insertOnly =
        WatchOptions.default[Person].withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))))

      requirePort()
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](connectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people_filtered")
          fiber      <- collection.watch(insertOnly).take(1).compile.toList.start

          _ <- IO.sleep(500.millis)

          _      <- collection.insertOne(Person("alice", 25))
          _      <- collection.updateOne(Field.of[Person, String](_.name).equalTo("alice"), Field.of[Person, Int](_.age).set(26))
          events <- fiber.joinWithNever
          _      <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT)
      }
    }

    "resume after a token, replaying only what followed it" in {
      requirePort()
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](connectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people_resumed")

          first <- collection.watch().take(1).compile.toList.start

          _ <- IO.sleep(500.millis)

          _    <- collection.insertOne(Person("first", 1))
          seen <- first.joinWithNever

          rest <- collection.watch(WatchOptions.resumeAfter[Person](seen.head.resumeToken)).take(2).compile.toList.start
          _    <- IO.sleep(500.millis)
          _    <- collection.insertOne(Person("second", 2))
          _    <- collection.insertOne(Person("third", 3))
          more <- rest.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting { events =>
        events.flatMap(_.fullDocument).map(_.name) shouldBe List("second", "third")
      }
    }

    "surface an undecodable event as a Left rather than killing the stream" in {
      requirePort()
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](connectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people_attempting")

          fiber <- collection.watchAttempting().take(2).compile.toList.start

          _ <- IO.sleep(500.millis)

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
