package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.BsonDocument

import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object NamingItSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Person(firstName: String, age: Int) derives WireCodec

final class NamingItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import NamingItSpec.Person

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val firstName = Field.of[Person, String](_.firstName)

  "a collection whose codec renames fields" should {

    "answer a query built from the same field, without being told the naming twice" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("naming_it")
          collection <- database.getDirectCollection[Person]("people")
          _          <- collection.insertOne(Person("bob", 30))
          found      <- collection.find(firstName.equalTo("bob")).all
        yield found

      program.timeout(30.seconds).asserting(_ shouldBe List(Person("bob", 30)))
    }

    "store the renamed field, so the query really did have to match it" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("naming_it")
          collection <- database.getDirectCollection[Person]("stored")
          raw        <- database.getCollection[BsonDocument]("stored")
          _          <- collection.insertOne(Person("bob", 30))
          document   <- raw.find().first
        yield document.map(_.keySet.toArray.toList.map(_.toString).filterNot(_ == "_id"))

      program.timeout(30.seconds).asserting(_ shouldBe Some(List("first_name", "age")))
    }

    "refuse a naming that does not spell the names its codec writes, instead of answering nothing" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("naming_it")
          refused  <- database.getDirectCollection[Person]("mismatch", mongo4s.bson.FieldNaming.identity).attempt
        yield refused

      program.timeout(30.seconds).asserting {
        case Left(error)  => error.getMessage should include("does not write the field names")
        case Right(value) => fail(s"expected the mismatch to be refused, got $value")
      }
    }

    "still take an explicit naming that agrees with the codec" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("naming_it")
          collection <- database.getDirectCollection[Person]("explicit", mongo4s.bson.FieldNaming.snakeCase)
          _          <- collection.insertOne(Person("bob", 30))
          found      <- collection.find(firstName.equalTo("bob")).all
        yield found

      program.timeout(30.seconds).asserting(_ shouldBe List(Person("bob", 30)))
    }

    "leave a hand-written codec alone, since it makes no claim about spelling" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("naming_it")
          opened   <- database.getCollection[BsonDocument]("handwritten", mongo4s.bson.FieldNaming.kebabCase).attempt
        yield opened

      program.timeout(30.seconds).asserting(_.isRight shouldBe true)
    }
  }
