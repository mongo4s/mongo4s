package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.{Deferred, IO}
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.{MongoClient, MongoSession, withTransaction}

import scala.concurrent.duration.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object TransactionItSpec:

  final case class Person(name: String, age: Int)

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person => BsonDocument().append("name", BsonString(person.name)).append("age", BsonInt32(person.age)),
      document =>
        for
          name <- field[String](document, "name")
          age  <- field[Int](document, "age")
        yield Person(name, age),
    )

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

final class TransactionItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import TransactionItSpec.Person

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  "MongoClient sessions/transactions" should {

    "keep a write invisible outside the session until commitTransaction, then visible everywhere" in {
      val program =
        for
          client      <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database    <- client.getDatabase("tx-commit")
          collection  <- database.getCollection[Person]("people")
          session     <- client.startSession
          _           <- MongoSession.startTransaction[IO](session)
          _           <- collection.insertOne(Person("bob", 30))(using Some(session))
          duringTx    <- collection.count()
          _           <- MongoSession.commitTransaction[IO, S](session)
          afterCommit <- collection.count()
          _           <- client.close
        yield (duringTx, afterCommit)

      program.timeout(30.seconds).asserting { case (duringTx, afterCommit) =>
        duringTx shouldBe 0L
        afterCommit shouldBe 1L
      }
    }

    "roll back a write on abortTransaction, leaving nothing visible" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("tx-abort")
          collection <- database.getCollection[Person]("people")
          session    <- client.startSession
          _          <- MongoSession.startTransaction[IO](session)
          _          <- collection.insertOne(Person("alice", 25))(using Some(session))
          _          <- MongoSession.abortTransaction[IO, S](session)
          afterAbort <- collection.count()
          _          <- client.close
        yield afterAbort

      program.timeout(30.seconds).asserting(_ shouldBe 0L)
    }
  }

  "MongoClient.withTransaction" should {

    "commit a successful body, with the session picked up implicitly by every call inside it" in {
      val program =
        for
          client      <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database    <- client.getDatabase("tx-helper-commit")
          collection  <- database.getCollection[Person]("people")
          _           <- client.withTransaction(collection.insertOne(Person("carol", 40)))
          afterCommit <- collection.count()
          _           <- client.close
        yield afterCommit

      program.timeout(30.seconds).asserting(_ shouldBe 1L)
    }

    "abort and re-raise when the body fails, leaving nothing visible" in {
      final case class Boom() extends RuntimeException("boom")

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("tx-helper-abort")
          collection <- database.getCollection[Person]("people")
          outcome    <- client.withTransaction(collection.insertOne(Person("dave", 50)) *> IO.raiseError(Boom())).attempt
          afterAbort <- collection.count()
          _          <- client.close
        yield (outcome, afterAbort)

      program.timeout(30.seconds).asserting { case (outcome, afterAbort) =>
        outcome.left.map(_.getMessage) shouldBe Left("boom")
        afterAbort shouldBe 0L
      }
    }

    "roll back and end the transaction when the fiber is cancelled" in {
      val program =
        for
          client      <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database    <- client.getDatabase("tx-cancel")
          collection  <- database.getCollection[Person]("people")
          session     <- client.startSession
          inserted    <- Deferred[IO, Unit]
          fiber       <- session
                           .withTransaction[IO, S, Unit](
                             collection.insertOne(Person("erin", 60)).void *> inserted.complete(()).void *> IO.never[Unit]
                           )
                           .start
          _           <- inserted.get
          _           <- fiber.cancel
          stillActive <- IO(session.hasActiveTransaction)
          afterCancel <- collection.count()
          _           <- IO(session.close())
          _           <- client.close
        yield (stillActive, afterCancel)

      program.timeout(30.seconds).asserting { case (stillActive, afterCancel) =>
        stillActive shouldBe false
        afterCancel shouldBe 0L
      }
    }
  }
