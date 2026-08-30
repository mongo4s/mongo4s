package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import com.mongodb.{ConnectionString, MongoClientSettings}
import org.bson.{BsonDocument, BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecConfigurationException, CodecRegistries}

import mongo4s.cats.CatsStream
import mongo4s.bson.direct.WireCodec
import mongo4s.{MongoClient, RsBridge}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object CodecRegistryItSpec:
  final case class Person(name: String, age: Int) derives WireCodec

  val shadowing: Codec[Person] = new Codec[Person]:
    def encode(writer: BsonWriter, value: Person, context: EncoderContext): Unit =
      writer.writeStartDocument()
      writer.writeName("shadowed")
      writer.writeString(value.name)
      writer.writeEndDocument()

    def decode(reader: BsonReader, context: DecoderContext): Person =
      reader.readStartDocument()
      reader.readName()
      val name = reader.readString()
      reader.readEndDocument()
      Person(name, -1)

    def getEncoderClass: Class[Person] = classOf[Person]

final class CodecRegistryItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CodecRegistryItSpec.{Person, shadowing}

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def clientWithShadowingRegistry: IO[MongoClient[IO, S]] =
    MongoClient.fromSettings[IO, S](
      MongoClientSettings
        .builder()
        .applyConnectionString(ConnectionString(container.getConnectionString))
        .codecRegistry(
          CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs(shadowing),
            MongoClientSettings.getDefaultCodecRegistry,
          )
        )
        .build()
    )

  "getDirectCollection" should {
    "use the derived WireCodec even when the client registry has a codec for the same type" in {
      val program =
        for
          client     <- clientWithShadowingRegistry
          database   <- client.getDatabase("codec_registry_it")
          collection <- database.getDirectCollection[Person]("direct")
          _          <- collection.insertOne(Person("bob", 30))
          stored     <- RsBridge[IO, S].list(collection.underlying.find())
          read       <- collection.find().all
          _          <- client.close
        yield (stored.map(_.toJson), read)

      program.asserting { (stored, read) =>
        stored.head should include(""""name": "bob"""")
        stored.head should include(""""age": 30""")
        stored.head should not include "shadowed"
        read shouldBe List(Person("bob", 30))
      }
    }
  }

  "a registry that replaces the driver's defaults instead of extending them" should {
    "fail every operation, because BsonDocument itself is no longer resolvable" in {
      val program =
        for
          client     <- MongoClient.fromSettings[IO, S](
                          MongoClientSettings
                            .builder()
                            .applyConnectionString(ConnectionString(container.getConnectionString))
                            .codecRegistry(CodecRegistries.fromCodecs(shadowing)) // no defaults behind it
                            .build()
                        )
          database   <- client.getDatabase("codec_registry_it")
          collection <- database.getCollection[CoreItSpec.Person]("replaced_defaults")
          inserted   <- collection.insertOne(CoreItSpec.Person("alice", 25)).attempt
          found      <- collection.find().all.attempt
          _          <- client.close
        yield (inserted, found)

      program.asserting { (inserted, found) =>
        inserted.left.map(_.getClass) shouldBe Left(classOf[CodecConfigurationException])
        found.left.map(_.getClass) shouldBe Left(classOf[CodecConfigurationException])
      }
    }
  }

  "the underlying escape hatch" should {
    "resolve through the client registry, mongo4s no longer in the way" in {
      val program =
        for
          client     <- clientWithShadowingRegistry
          database   <- client.getDatabase("codec_registry_it")
          collection <- database.getCollection[CoreItSpec.Person]("escape_hatch")
          typed       = collection.underlying.withDocumentClass(classOf[Person])
          _          <- RsBridge[IO, S].one(typed.insertOne(Person("bob", 30)))
          stored     <- RsBridge[IO, S].list(collection.underlying.find())
          _          <- client.close
        yield stored.map(_.toJson)

      program.asserting(_.head should include("shadowed"))
    }
  }

  "the rest of the client" should {
    "keep working through the BsonDocument path, which the registry never decides" in {
      val program =
        for
          client     <- clientWithShadowingRegistry
          database   <- client.getDatabase("codec_registry_it")
          collection <- database.getCollection[CoreItSpec.Person]("documents")
          _          <- collection.insertOne(CoreItSpec.Person("alice", 25))
          found      <- collection.find().all
          indexes    <- collection.listIndexes
          command    <- database.runCommand(BsonDocument("ping", org.bson.BsonInt32(1)))
          _          <- client.close
        yield (found, indexes.size, command.getDouble("ok").getValue)

      program.asserting { (found, indexCount, ok) =>
        found shouldBe List(CoreItSpec.Person("alice", 25))
        indexCount shouldBe 1 // the implicit _id index
        ok shouldBe 1.0
      }
    }
  }
