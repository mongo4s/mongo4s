package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient}
import mongo4s.operations.{Sort, Stage}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

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
          count      <- collection.count()
          all        <- collection.find().all
          bob        <- collection.find(Field.of[Person, String](_.name).equalTo("bob")).first
          streamed   <- collection.find().stream.compile.toList
          aggregated <- collection
                          .aggregate[Person](
                            Seq(
                              Stage.matching(Field.of[Person, Int](_.age).gte(28)),
                              Stage.sortBy(Sort.asc(Field.of[Person, String](_.name))),
                            )
                          )
                          .all
          _          <- client.close
        yield (count, all, bob, streamed, aggregated)

      program.timeout(30.seconds).asserting { case (count, all, bob, streamed, aggregated) =>
        count shouldBe 2L
        all.map(_.name) should contain allOf ("bob", "alice")
        bob shouldBe Some(Person("bob", 30))
        streamed.map(_.name) should contain allOf ("bob", "alice")
        aggregated shouldBe List(Person("bob", 30))
      }
    }

    "decode errors surface as a failed effect" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("broken")
          collection <- database.getCollection[Person]("people")
          _          <- collection.underlying.insertOne(BsonDocument().append("name", BsonString("no-age"))).pure0
          result     <- collection.find().all.attempt
          _          <- client.close
        yield result

      program.timeout(30.seconds).asserting(_.isLeft shouldBe true)
    }
  }

  extension [A](publisher: org.reactivestreams.Publisher[A])
    private def pure0: IO[Unit] =
      fs2.interop.reactivestreams.fromPublisher[IO, A](publisher, 1).compile.drain
