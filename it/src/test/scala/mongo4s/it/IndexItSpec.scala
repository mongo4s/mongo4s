package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.types.ObjectId
import org.bson.{BsonDocument, BsonInt32}

import mongo4s.cats.CatsStream
import mongo4s.operations.Index
import mongo4s.repositories.BaseMongoRepository
import mongo4s.{Field, MongoClient, MongoCollection, WithId}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given

final class IndexItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val nameField = Field.of[Person, String](_.name)
  private val ageField  = Field.of[Person, Int](_.age)

  private def collection(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("index_it")
      collection <- database.getCollection[Person](name)
    yield collection

  private def byName(indexes: List[BsonDocument]): Map[String, BsonDocument] =
    indexes.flatMap(i => Option(i.getString("name", null)).map(_.getValue -> i)).toMap

  "createIndex" should {

    "create a compound index whose key order and directions survive the round trip" in {
      val program =
        for
          people  <- collection("compound")
          _       <- people.createIndex(Index.ascending(nameField).descending(ageField).named("name_age"))
          indexes <- people.listIndexes
        yield byName(indexes).get("name_age").map(_.getDocument("key").toJson)

      program.asserting(_ shouldBe Some("""{"name": 1, "age": -1}"""))
    }

    "create a unique index that the server then enforces" in {
      val program =
        for
          people <- collection("unique")
          _      <- people.createIndex(Index.unique(nameField).named("name_unique"))
          _      <- people.insertOne(Person("bob", 30))
          failed <- people.insertOne(Person("bob", 31)).attempt
        yield failed.isLeft

      program.asserting(_ shouldBe true)
    }

    "create a partial index, and report the filter back" in {
      val program =
        for
          people  <- collection("partial")
          _       <- people.createIndex(Index.ascending(ageField).named("adults_only").where(ageField.gte(18)))
          indexes <- people.listIndexes
        yield byName(indexes).get("adults_only").map(_.getDocument("partialFilterExpression").toJson)

      program.asserting(_ shouldBe Some("""{"age": {"$gte": 18}}"""))
    }

    "create a TTL index with the seconds the duration describes" in {
      val program =
        for
          people  <- collection("ttl")
          _       <- people.createIndex(Index.ascending(ageField).named("expires").expiringAfter(2.hours))
          indexes <- people.listIndexes
        yield byName(indexes).get("expires").map(_.getNumber("expireAfterSeconds").longValue)

      program.asserting(_ shouldBe Some(7200L))
    }

    "create a TTL index that expires each document at the time its field holds" in {
      val program =
        for
          people  <- collection("ttl_at_field_time")
          _       <- people.createIndex(Index.ascending(ageField).named("due").expiringAtFieldTime)
          indexes <- people.listIndexes
        yield byName(indexes).get("due").map(_.getNumber("expireAfterSeconds").longValue)

      program.asserting(_ shouldBe Some(0L))
    }

    "create a sparse index" in {
      val program =
        for
          people  <- collection("sparse")
          _       <- people.createIndex(Index.ascending(ageField).withSparse.named("age_sparse"))
          indexes <- people.listIndexes
        yield byName(indexes).get("age_sparse").map(_.getBoolean("sparse").getValue)

      program.asserting(_ shouldBe Some(true))
    }

    "return the server's name for the index it created" in {
      collection("auto_named").flatMap(_.createIndex(Index.ascending(ageField))).asserting(_ shouldBe "age_1")
    }
  }

  "listIndexes and dropIndex" should {

    "report the implicit _id index alongside the created ones, and drop only what was asked for" in {
      val program =
        for
          people <- collection("lifecycle")
          _      <- people.insertOne(Person("bob", 30))
          _      <- people.createIndex(Index.ascending(ageField).named("age_asc"))
          before <- people.listIndexes
          _      <- people.dropIndex("age_asc")
          after  <- people.listIndexes
        yield (byName(before).keySet, byName(after).keySet)

      program.asserting { (before, after) =>
        before should contain allOf ("_id_", "age_asc")
        after should contain("_id_")
        after should not contain "age_asc"
      }
    }

    "fail rather than pretend when the index does not exist" in {
      val program =
        for
          people <- collection("missing_index")
          _      <- people.insertOne(Person("bob", 30))
          failed <- people.dropIndex("nope").attempt
        yield failed.isLeft

      program.asserting(_ shouldBe true)
    }
  }

  "createIndexes" should {

    "build every index in one command and return the names in the order they were given" in {
      val program =
        for
          people  <- collection("several")
          names   <- people.createIndexes(List(Index.ascending(nameField).named("by_name"), Index.descending(ageField)))
          indexes <- people.listIndexes
        yield (names, byName(indexes).keySet)

      program.asserting { (names, existing) =>
        names shouldBe List("by_name", "age_-1")
        existing shouldBe Set("_id_", "by_name", "age_-1")
      }
    }

    "build none of them when one cannot be built" in {
      val program =
        for
          people  <- collection("all_or_nothing")
          _       <- people.insertMany(List(Person("bob", 30), Person("bob", 31)))
          failed  <- people.createIndexes(List(Index.ascending(ageField).named("fine"), Index.unique(nameField).named("clash"))).attempt
          indexes <- people.listIndexes
        yield (failed.isLeft, byName(indexes).keySet)

      program.asserting { (failed, existing) =>
        failed shouldBe true
        existing shouldBe Set("_id_")
      }
    }

    "leave an index that already exists as it is, and still report its name" in {
      val program =
        for
          people <- collection("partly_existing")
          _      <- people.createIndex(Index.ascending(ageField).named("by_age"))
          names  <- people.createIndexes(List(Index.ascending(ageField).named("by_age"), Index.ascending(nameField)))
        yield names

      program.asserting(_ shouldBe List("by_age", "name_1"))
    }

    "return nothing for no indexes, without asking the server" in {
      collection("no_indexes").flatMap(_.createIndexes(Nil)).asserting(_ shouldBe Nil)
    }
  }

  "dropIndexes" should {

    "drop every index except the one on _id" in {
      val program =
        for
          people  <- collection("drop_all")
          _       <- people.createIndexes(List(Index.ascending(nameField), Index.ascending(ageField)))
          _       <- people.dropIndexes
          indexes <- people.listIndexes
        yield byName(indexes).keySet

      program.asserting(_ shouldBe Set("_id_"))
    }
  }

  "Repository.ensureKeyIndex" should {
    import RepositoryItSpec.Person.given

    "build the unique index the PrimaryKey describes" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("index_it")
          people     <- BaseMongoRepository.create[IO, S, RepositoryItSpec.Person, String](database, "ensure_key_index")
          _          <- people.ensureKeyIndex
          _          <- people.insertOne(RepositoryItSpec.Person("1", "bob", 30))
          duplicated <- people.insertOne(RepositoryItSpec.Person("1", "other", 40)).attempt
        yield duplicated.isLeft

      program.asserting(_ shouldBe true)
    }

    "answer _id_ for a key stored as _id, where the server refuses a unique index" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("index_it")
          notes    <- BaseMongoRepository.objectId[IO, S, RepositoryItSpec.Note](database, "ensure_id_index")
          name     <- notes.ensureKeyIndex
          _        <- notes.insertOne(WithId(ObjectId.get(), RepositoryItSpec.Note("kept")))
          indexes  <- database.getCollection[BsonDocument]("ensure_id_index").flatMap(_.listIndexes)
        yield (name, byName(indexes).keySet)

      program.asserting(_ shouldBe ("_id_", Set("_id_")))
    }
  }

  "a text index" should {
    "carry its weights, its default language and the field that overrides it" in {
      val bioField  = Field.stored[Person, String]("bio")
      val langField = Field.stored[Person, String]("lang")

      val program =
        for
          people  <- collection("text_weights")
          _       <- people.createIndex(
                       Index
                         .text(nameField)
                         .text(bioField)
                         .withWeight(nameField, 10)
                         .withDefaultLanguage("spanish")
                         .withLanguageOverride(langField)
                         .named("by_text")
                     )
          indexes <- people.listIndexes
          text     = byName(indexes)("by_text")
        yield (text.getDocument("weights"), text.getString("default_language").getValue, text.getString("language_override").getValue)

      program.asserting(_ shouldBe (BsonDocument("bio", BsonInt32(1)).append("name", BsonInt32(10)), "spanish", "lang"))
    }
  }

  "the index types the server validates" should {
    "accept hashed, 2dsphere, wildcard and hidden specifications" in {
      val program =
        for
          people   <- collection("index_types")
          _        <- people.createIndex(Index.hashed(nameField).named("by_hash"))
          _        <- people.createIndex(Index.geo2DSphere(Field.stored[Person, Any]("location")).named("by_area"))
          _        <- people.createIndex(Index.ascending(Field.stored[Person, Any]("$**")).named("by_anything"))
          _        <- people.createIndex(Index.ascending(ageField).named("by_age").withHidden)
          existing <- people.listIndexes
        yield byName(existing)

      program.asserting { indexes =>
        indexes.keySet should contain allOf ("by_hash", "by_area", "by_anything", "by_age")
        indexes("by_hash").getDocument("key").getString("name").getValue shouldBe "hashed"
        indexes("by_area").getDocument("key").getString("location").getValue shouldBe "2dsphere"
        indexes("by_age").getBoolean("hidden").getValue shouldBe true
      }
    }
  }
