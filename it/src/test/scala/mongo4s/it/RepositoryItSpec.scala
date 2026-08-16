package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.types.ObjectId
import mongo4s.bson.medeia.MedeiaDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.{Update, WriteCommand}
import mongo4s.repositories.BaseMongoRepository
import mongo4s.{Field, MongoClient, PrimaryKey, WithId}

import scala.concurrent.duration.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given

object RepositoryItSpec:

  final case class Person(id: String, name: String, age: Int) derives MedeiaDocumentCodec

  object Person:
    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

  final case class Note(text: String) derives MedeiaDocumentCodec

final class RepositoryItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import RepositoryItSpec.{Note, Person}
  import RepositoryItSpec.Person.given

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def repository(dbName: String): IO[(MongoClient[IO, S], BaseMongoRepository[IO, S, Person, String])] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase(dbName)
      repository <- BaseMongoRepository.create[IO, S, Person, String](database, "people")
    yield (client, repository)

  "BaseMongoRepository.create (custom string key) against a real MongoDB" should {

    "insertOne / findOne round-trip an entity" in {
      (for
        (client, repo) <- repository("repo_it_find_one")
        _              <- repo.insertOne(Person("1", "bob", 30))
        found          <- repo.findOne("1")
        missing        <- repo.findOne("missing")
        _              <- client.close
      yield (found, missing)).timeout(30.seconds).asserting { case (found, missing) =>
        found shouldBe Some(Person("1", "bob", 30))
        missing shouldBe None
      }
    }

    "insertMany / findMany / findBy / findByFilter batch and filter correctly" in {
      (for
        (client, repo) <- repository("repo_it_find_many")
        _              <- repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25), Person("3", "eve", 40)))
        many           <- repo.findMany(List("1", "3"))
        byName         <- repo.findBy(Field.of[Person, String](_.name), "alice")
        byFilter       <- repo.findByFilter(Field.of[Person, Int](_.age).gt(28))
        _              <- client.close
      yield (many, byName, byFilter)).timeout(30.seconds).asserting { case (many, byName, byFilter) =>
        many should contain theSameElementsAs List(Person("1", "bob", 30), Person("3", "eve", 40))
        byName shouldBe List(Person("2", "alice", 25))
        byFilter.map(_.id) should contain allOf ("1", "3")
      }
    }

    "getAll / getBy stream matching documents" in {
      (for
        (client, repo) <- repository("repo_it_streaming")
        _              <- repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        all            <- repo.getAll.compile.toList
        filtered       <- repo.getBy(Field.of[Person, String](_.name).equalTo("alice")).compile.toList
        _              <- client.close
      yield (all, filtered)).timeout(30.seconds).asserting { case (all, filtered) =>
        all should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25))
        filtered shouldBe List(Person("2", "alice", 25))
      }
    }

    "upsert / upsertMany insert new keys and replace existing ones" in {
      (for
        (client, repo) <- repository("repo_it_upsert")
        _              <- repo.upsert(Person("1", "bob", 30))
        _              <- repo.upsert(Person("1", "bob", 31))
        _              <- repo.upsertMany(List(Person("2", "alice", 25)))
        found          <- repo.findOne("1")
        found2         <- repo.findOne("2")
        _              <- client.close
      yield (found, found2)).timeout(30.seconds).asserting { case (found, found2) =>
        found shouldBe Some(Person("1", "bob", 31))
        found2 shouldBe Some(Person("2", "alice", 25))
      }
    }

    "updateField / updateBy apply real Mongo update operators" in {
      (for
        (client, repo) <- repository("repo_it_update")
        _              <- repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 30), Person("3", "eve", 40)))
        _              <- repo.updateField("1", Field.of[Person, Int](_.age), 99)
        modified       <- repo.updateBy(Field.of[Person, Int](_.age).equalTo(30), Update.set(Field.of[Person, Int](_.age), 50))
        one            <- repo.findOne("1")
        two            <- repo.findOne("2")
        _              <- client.close
      yield (modified, one, two)).timeout(30.seconds).asserting { case (modified, one, two) =>
        // "1" was already moved off age=30 by updateField, so updateBy's age==30 filter only still matches "2"
        modified shouldBe 1L
        one shouldBe Some(Person("1", "bob", 99))
        two shouldBe Some(Person("2", "alice", 50))
      }
    }

    "bulkWrite applies mixed write commands" in {
      (for
        (client, repo) <- repository("repo_it_bulk")
        _              <- repo.insertOne(Person("1", "bob", 30))
        _              <- repo.bulkWrite(
                            Seq(
                              WriteCommand.InsertOne(Person("2", "alice", 25)),
                              WriteCommand.DeleteOne(Field.of[Person, String](_.id).equalTo("1")),
                            )
                          )
        all            <- repo.getAll.compile.toList
        _              <- client.close
      yield all).timeout(30.seconds).asserting(_ shouldBe List(Person("2", "alice", 25)))
    }

    "deleteOne / deleteMany / count remove and report correctly" in {
      (for
        (client, repo) <- repository("repo_it_delete")
        _              <- repo.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3)))
        _              <- repo.deleteOne("1")
        _              <- repo.deleteMany(List("2", "3"))
        remaining      <- repo.count()
        _              <- client.close
      yield remaining).timeout(30.seconds).asserting(_ shouldBe 0L)
    }
  }

  "BaseMongoRepository.objectId (auto _id via WithId) against a real MongoDB" should {
    "round-trip an entity keyed by a real Mongo ObjectId" in {
      val oid = ObjectId.get()
      (for
        client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
        database   <- client.getDatabase("repo_it_oid")
        repository <- BaseMongoRepository.objectId[IO, S, Note](database, "notes")
        _          <- repository.insertOne(WithId(oid, Note("hello")))
        found      <- repository.findOne(oid)
        _          <- client.close
      yield found).timeout(30.seconds).asserting(_ shouldBe Some(WithId(oid, Note("hello"))))
    }
  }
