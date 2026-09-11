package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import com.mongodb.reactivestreams.client.ClientSession
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
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
