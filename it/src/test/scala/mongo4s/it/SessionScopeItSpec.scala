package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.{IO, Ref}
import com.mongodb.reactivestreams.client.ClientSession
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.SessionOptions
import mongo4s.{MongoClient, MongoCollection, withTransaction}

import scala.concurrent.duration.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object SessionScopeItSpec:

  final case class Person(name: String) derives CanEqual

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person => BsonDocument().append("name", BsonString(person.name)),
      document => Option(document.get("name")).toRight(BsonError.MissingField("name")).flatMap(BsonDecoder[String].decode).map(Person.apply),
    )

  type S[A] = CatsStream[IO][A]

  /** Written the way a helper has to be written to join its caller's transaction. */
  def insertInSession(collection: MongoCollection[IO, S, Person], person: Person)(using Option[ClientSession]): IO[Unit] =
    collection.insertOne(person).void

  /** Written the way that silently escapes it. */
  def insertOutsideSession(collection: MongoCollection[IO, S, Person], person: Person): IO[Unit] =
    collection.insertOne(person).void

final class SessionScopeItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import SessionScopeItSpec.*

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  private def collection(name: String): IO[(MongoClient[IO, S], MongoCollection[IO, S, Person])] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("session_scope_it")
      collection <- database.getCollection[Person](name)
      _          <- database.createCollection(name).attempt.void
    yield (client, collection)

  private def rolledBack(name: String)(body: MongoCollection[IO, S, Person] => Option[ClientSession] ?=> IO[Unit]): IO[List[Person]] =
    for
      (client, people) <- collection(name)
      _                <- client.withTransaction(body(people) *> IO.raiseError(RuntimeException("rollback"))).attempt
      survivors        <- people.find().all
      _                <- client.close
    yield survivors

  "a helper that takes the session" should {

    "have its write rolled back with the transaction" in {
      rolledBack("joins")(people => insertInSession(people, Person("bob")))
        .timeout(60.seconds)
        .asserting(_ shouldBe Nil)
    }
  }

  "a helper that does not take the session" should {

    "commit outside the transaction, which is the trap the docs have to name" in {
      rolledBack("escapes")(people => insertOutsideSession(people, Person("bob")))
        .timeout(60.seconds)
        .asserting(_ shouldBe List(Person("bob")))
    }
  }

  "a call built before the transaction" should {

    "commit outside it even when it runs inside, because a call takes its session when it is built" in {
      val program =
        for
          (client, people) <- collection("prebuilt")
          write             = people.insertOne(Person("bob")).void
          _                <- client.withTransaction(write *> IO.raiseError(RuntimeException("rollback"))).attempt
          survivors        <- people.find().all
          _                <- client.close
        yield survivors

      program.timeout(60.seconds).asserting(_ shouldBe List(Person("bob")))
    }
  }

  "withSession" should {

    "give its session to every call inside, so a snapshot session reads one point in time throughout" in {
      val program =
        for
          (client, people) <- collection("snapshot")
          _                <- people.insertOne(Person("before"))
          seen             <- client.withSession(
                                for
                                  first  <- people.find().all
                                  _      <- insertOutsideSession(people, Person("after"))
                                  second <- people.find().all
                                yield (first, second),
                                SessionOptions.snapshot,
                              )
          everything       <- people.find().all
          _                <- client.close
        yield (seen, everything)

      program.timeout(60.seconds).asserting { case ((first, second), everything) =>
        first shouldBe List(Person("before"))
        second shouldBe first
        everything should have size 2
      }
    }

    "close its session when the body fails" in {
      val program =
        for
          (client, _) <- collection("closed")
          opened      <- Ref.of[IO, Option[ClientSession]](None)
          failed      <- client.withSession(opened.set(summon[Option[ClientSession]]) *> IO.raiseError[Unit](RuntimeException("boom"))).attempt
          session     <- opened.get
          _           <- client.close
        yield (failed.isLeft, session.map(closed => scala.util.Try(closed.getServerSession).isFailure))

      program.timeout(60.seconds).asserting(_ shouldBe (true, Some(true)))
    }
  }
