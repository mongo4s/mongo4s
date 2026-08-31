package mongo4s.it.watch

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonBoolean, BsonDocument, BsonString}
import com.mongodb.client.model.changestream.{FullDocument, FullDocumentBeforeChange}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.Stage
import mongo4s.changestream.WatchOptions
import mongo4s.{Field, MongoClient}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class WatchOptionsItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import WatchFixture.Person

  type S[A] = CatsStream[IO][A]

  private val ageOf  = Field.of[Person, Int](_.age)
  private val nameOf = Field.of[Person, String](_.name)

  private val updatesOnly: Seq[Stage[Person]] = Seq(Stage.raw[Person](BsonDocument("$match", BsonDocument("operationType", BsonString("update")))))

  "resumeAfter" should {

    "replay only what followed the token" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options")
          collection <- database.getCollection[Person]("resume_after")

          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          first <- collection.watch(WatchOptions.default[Person].startingAt(now)).take(1).compile.toList.start
          _     <- collection.insertOne(Person("first", 1))
          seen  <- first.joinWithNever

          rest <- collection.watch(WatchOptions.resumeAfter[Person](seen.head.resumeToken)).take(2).compile.toList.start
          _    <- IO.sleep(500.millis)
          _    <- collection.insertOne(Person("second", 2))
          _    <- collection.insertOne(Person("third", 3))
          more <- rest.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting(_.flatMap(_.fullDocument).map(_.name) shouldBe List("second", "third"))
    }
  }

  "startingAfter" should {

    "resume from a token the same way, and clear a previously set resumeAfter" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options")
          collection <- database.getCollection[Person]("starting_after")

          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          first <- collection.watch(WatchOptions.default[Person].startingAt(now)).take(1).compile.toList.start
          _     <- collection.insertOne(Person("first", 1))
          seen  <- first.joinWithNever

          options = WatchOptions.resumeAfter[Person](seen.head.resumeToken).startingAfter(seen.head.resumeToken)
          rest   <- collection.watch(options).take(1).compile.toList.start
          _      <- IO.sleep(500.millis)
          _      <- collection.insertOne(Person("second", 2))
          more   <- rest.joinWithNever

          _ <- client.close
        yield (options, more)

      program.timeout(30.seconds).asserting { (options, events) =>
        options.resumeAfter shouldBe None
        options.startAfter.isDefined shouldBe true
        events.flatMap(_.fullDocument).map(_.name) shouldBe List("second")
      }
    }
  }

  "startingAt" should {

    "replay events at or after a cluster time" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options")
          collection <- database.getCollection[Person]("starting_at")

          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          first <- collection.watch(WatchOptions.default[Person].startingAt(now)).take(1).compile.toList.start
          _     <- collection.insertOne(Person("marker", 0))
          seen  <- first.joinWithNever

          from  = seen.head.clusterTime.get
          rest <- collection.watch(WatchOptions.default[Person].startingAt(from)).take(1).compile.toList.start
          _    <- IO.sleep(500.millis)
          more <- rest.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting(_.flatMap(_.fullDocument).map(_.name) shouldBe List("marker"))
    }
  }

  "withFullDocument" should {

    "leave fullDocument empty on updates when set back to the server default" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options")
          collection <- database.getCollection[Person]("full_document_default")

          _     <- collection.insertOne(Person("bob", 30))
          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber <-
            collection.watch(WatchOptions.default[Person].withFullDocument(FullDocument.DEFAULT).withPipeline(updatesOnly).startingAt(now)).take(1).compile.toList.start
          _     <- collection.updateOne(nameOf.equalTo("bob"), ageOf.set(31))
          more  <- fiber.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting(_.head.fullDocument shouldBe None)
    }
  }

  "withFullDocumentBeforeChange" should {

    "return the pre-image of an updated document when the collection keeps them" in {
      val enablePreImages =
        BsonDocument()
          .append("collMod", BsonString("pre_images"))
          .append("changeStreamPreAndPostImages", BsonDocument().append("enabled", BsonBoolean(true)))

      val options =
        WatchOptions.default[Person].withFullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED).withPipeline(updatesOnly)

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options_pre_images")
          collection <- database.getCollection[Person]("pre_images")

          _ <- collection.insertOne(Person("bob", 30))
          _ <- database.runCommand(enablePreImages)

          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber <- collection.watch(options.startingAt(now)).take(1).compile.toList.start
          _     <- collection.updateOne(nameOf.equalTo("bob"), ageOf.set(31))
          more  <- fiber.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting { events =>
        events.head.fullDocumentBeforeChange shouldBe Some(Person("bob", 30))
        events.head.fullDocument shouldBe Some(Person("bob", 31))
      }
    }
  }

  "withMaxAwaitTime and withBatchSize" should {

    "still deliver every event when both are set" in {
      val options = WatchOptions.default[Person].withMaxAwaitTime(1.second).withBatchSize(2)

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](WatchFixture.connectionString)
          database   <- client.getDatabase("watch_options")
          collection <- database.getCollection[Person]("await_and_batch")

          now   <- database.runCommand(WatchFixture.Hello).map(_.getTimestamp("operationTime"))
          fiber <- collection.watch(options.startingAt(now)).take(3).compile.toList.start
          _     <- collection.insertOne(Person("one", 1))
          _     <- collection.insertOne(Person("two", 2))
          _     <- collection.insertOne(Person("three", 3))
          more  <- fiber.joinWithNever

          _ <- client.close
        yield more

      program.timeout(30.seconds).asserting(_.flatMap(_.fullDocument).map(_.name) shouldBe List("one", "two", "three"))
    }
  }
