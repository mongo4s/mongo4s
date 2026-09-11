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

  final case class Legacy(givenName: String)

  object Legacy:
    given WireCodec[Legacy] = new WireCodec[Legacy]:
      override def fieldNaming: mongo4s.bson.FieldNaming = mongo4s.bson.FieldNaming.kebabCase

      def encode(writer: org.bson.BsonWriter, value: Legacy): Unit =
        writer.writeStartDocument()
        writer.writeName("given-name")
        writer.writeString(value.givenName)
        writer.writeEndDocument()

      def decode(reader: org.bson.BsonReader): Legacy =
        reader.readStartDocument()
        var name = ""
        while reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT do if reader.readName() == "given-name" then name = reader.readString() else reader.skipValue()
        reader.readEndDocument()
        Legacy(name)

final class NamingItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import NamingItSpec.Person

  private val container = new MongoDBContainer("mongo:8.2")

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

    "take a hand-written codec's own spelling, since that is where it is declared" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("naming_it")
          collection <- database.getDirectCollection[NamingItSpec.Legacy]("legacy")
          _          <- collection.insertOne(NamingItSpec.Legacy("bob"))
          raw        <- database.getCollection[BsonDocument]("legacy")
          stored     <- raw.find().first
          found      <- collection.find(Field.of[NamingItSpec.Legacy, String](_.givenName).equalTo("bob")).all
        yield (stored.map(_.keySet.toArray.toList.map(_.toString).filterNot(_ == "_id")), found)

      program.timeout(30.seconds).asserting { (keys, found) =>
        keys shouldBe Some(List("given-name"))
        found shouldBe List(NamingItSpec.Legacy("bob"))
      }
    }

  }
