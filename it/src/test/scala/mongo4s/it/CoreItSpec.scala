package mongo4s.it

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import mongo4s.bson.*
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient}
import org.bson.{BsonDocument, BsonInt32, BsonString}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.testcontainers.containers.MongoDBContainer

import scala.concurrent.duration.*

object CoreItSpec:

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

final class CoreItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  "core driver layer" should {

    "insert documents and read them back" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("people")
          _          <- collection.insertOne(Person("bob", 30))
          _          <- collection.insertOne(Person("alice", 25))
          count      <- collection.count
          all        <- collection.find.all
          bob        <- collection.find(Field.of[Person, String](_.name).equalTo("bob")).first
          streamed   <- collection.find.stream.compile.toList
          _          <- client.close
        yield (count, all, bob, streamed)

      program.timeout(30.seconds).asserting { case (count, all, bob, streamed) =>
        count shouldBe 2L
        all.map(_.name) should contain allOf ("bob", "alice")
        bob shouldBe Some(Person("bob", 30))
        streamed.map(_.name) should contain allOf ("bob", "alice")
      }
    }

    "decode errors surface as a failed effect" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("broken")
          collection <- database.getCollection[Person]("people")
          _          <- collection.underlying.insertOne(BsonDocument().append("name", BsonString("no-age"))).pure0
          result     <- collection.find.all.attempt
          _          <- client.close
        yield result

      program.timeout(30.seconds).asserting(_.isLeft shouldBe true)
    }
  }

  extension [A](publisher: org.reactivestreams.Publisher[A])
    private def pure0: IO[Unit] =
      fs2.interop.reactivestreams.fromPublisher[IO, A](publisher, 1).compile.drain
