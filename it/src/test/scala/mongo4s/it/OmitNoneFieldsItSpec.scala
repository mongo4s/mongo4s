package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonNull, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.{MongoClient, RsBridge}
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object OmitNoneFieldsItSpec:

  final case class Contact(name: String, email: Option[String]) derives WireCodec

  object NullsKept:
    given WireCodecConfig = WireCodecConfig.Default.withOmitNoneFields(false)
    final case class LegacyContact(name: String, email: Option[String]) derives WireCodec

final class OmitNoneFieldsItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import OmitNoneFieldsItSpec.{Contact, NullsKept}

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  "a direct collection" should {

    "leave a None field out of the stored document entirely" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("omit_none_it")
          collection <- database.getDirectCollection[Contact]("omitted")
          _          <- collection.insertOne(Contact("bob", None))
          stored     <- RsBridge[IO, S].list(collection.underlying.find())
          read       <- collection.find().all
          _          <- client.close
        yield (stored, read)

      program.asserting { (stored, read) =>
        stored.head.containsKey("name") shouldBe true
        stored.head.containsKey("email") shouldBe false
        read shouldBe List(Contact("bob", None))
      }
    }

    "read a document that already carries an explicit null back as None, so a collection written before omitNoneFields keeps working" in {
      val legacy = BsonDocument().append("name", BsonString("alice")).append("email", BsonNull.VALUE)

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("omit_none_it")
          collection <- database.getDirectCollection[Contact]("mixed")
          _          <- RsBridge[IO, S].one(collection.underlying.insertOne(legacy))
          _          <- collection.insertOne(Contact("bob", None))
          read       <- collection.find().all
          _          <- client.close
        yield read

      program.asserting(_ should contain theSameElementsAs List(Contact("alice", None), Contact("bob", None)))
    }

    "store an explicit null when the entity was derived with omitNoneFields disabled" in {
      import NullsKept.LegacyContact

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("omit_none_it")
          collection <- database.getDirectCollection[LegacyContact]("nulls_kept")
          _          <- collection.insertOne(LegacyContact("carol", None))
          stored     <- RsBridge[IO, S].list(collection.underlying.find())
          read       <- collection.find().all
          _          <- client.close
        yield (stored, read)

      program.asserting { (stored, read) =>
        stored.head.isNull("email") shouldBe true
        read shouldBe List(LegacyContact("carol", None))
      }
    }
  }
