package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}

import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.cats.CatsStream
import mongo4s.operations.Stage
import mongo4s.{Field, MongoClient}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object NamingItSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Person(firstName: String, age: Int) derives WireCodec

  final case class Checked(firstName: String, age: Int)

  object Identity:
    given WireCodecConfig = WireCodecConfig.Default

    final case class Badge(holderName: String, badgeLevel: Int) derives WireCodec

  object Checked:
    given WireCodec[Checked] =
      WireCodec.derived[Checked].iemap(checked => Either.cond(checked.age >= 0, checked, "age cannot be negative"))(identity)

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

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

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

    "keep the derived spelling when the codec is wrapped in iemap to validate what it reads" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("naming_it")
          collection <- database.getDirectCollection[NamingItSpec.Checked]("checked")
          _          <- collection.insertOne(NamingItSpec.Checked("bob", 30))
          found      <- collection.find(Field.of[NamingItSpec.Checked, String](_.firstName).equalTo("bob")).all
        yield found

      program.timeout(30.seconds).asserting(_ shouldBe List(NamingItSpec.Checked("bob", 30)))
    }

  }

  "a join between collections with different namings" should {

    "match on the foreign collection's own spelling when it is given the collection" in {
      import NamingItSpec.Identity.Badge

      val holder = Field.of[Badge, String](_.holderName)
      val senior = Field.of[Badge, Int](_.badgeLevel).gte(2)

      val program =
        for
          client <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          db     <- client.getDatabase("naming_it")
          people <- db.getDirectCollection[Person]("join_people")
          badges <- db.getDirectCollection[Badge]("join_badges")
          _      <- people.insertOne(Person("ann", 30))
          _      <- badges.insertMany(List(Badge("ann", 3), Badge("ann", 1)))
          plain  <- people.aggregate[BsonDocument](Seq(Stage.lookup(badges, firstName, holder, "badges"))).all
          keyed  <- people.aggregate[BsonDocument](Seq(Stage.lookupWithKeys(badges, firstName, holder, List(Stage.matching(senior)), "badges"))).all
          graph  <- people.aggregate[BsonDocument](Seq(Stage.graphLookup[Person, Badge, String](badges, BsonString("$first_name"), holder, holder, "badges"))).all
        yield (badges.naming("holderName"), people.naming("firstName"), List(plain, keyed, graph).map(_.head.getArray("badges").size))

      program.timeout(30.seconds).asserting(_ shouldBe ("holderName", "first_name", List(2, 1, 2)))
    }
  }
