package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.{Field, PrimaryKey}
import mongo4s.testkit.FakeRepository
import mongo4s.operations.{Update, WriteCommand}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object BaseMongoRepositorySpec:

  final case class Person(id: String, name: String, age: Int)

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person =>
        BsonDocument()
          .append("id", BsonString(person.id))
          .append("name", BsonString(person.name))
          .append("age", BsonInt32(person.age)),
      document =>
        for
          id   <- field[String](document, "id")
          name <- field[String](document, "name")
          age  <- field[Int](document, "age")
        yield Person(id, name, age),
    )

    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

final class BaseMongoRepositorySpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import BaseMongoRepositorySpec.Person
  import BaseMongoRepositorySpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def repo(batchSize: Int = 500): FakeRepository[IO, S, Person, String] =
    FakeRepository[IO, S, Person, String](list => fs2.Stream.emits(list), batchSize)

  "findOne" should {
    "return the entity matching the key" in {
      val repository = repo()
      for
        _       <- repository.fake.insertOne(Person("1", "bob", 30))
        found   <- repository.findOne("1")
        missing <- repository.findOne("missing")
      yield
        found shouldBe Some(Person("1", "bob", 30))
        missing shouldBe None
    }
  }

  "findMany" should {
    "batch lookups across multiple round trips" in {
      val repository = repo(batchSize = 2)
      for
        _     <- repository.fake.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3)))
        found <- repository.findMany(List("1", "2", "3"))
      yield found should contain theSameElementsAs List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3))
    }
  }

  "findBy / findByFilter" should {
    "filter by a single field" in {
      val repository = repo()
      for
        _     <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        found <- repository.findBy(Field.of[Person, String](_.name), "bob")
      yield found shouldBe List(Person("1", "bob", 30))
    }

    "filter by an arbitrary Filter" in {
      val repository = repo()
      val filter     = Field.of[Person, Int](_.age).gte(28)
      for
        _     <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        found <- repository.findByFilter(filter)
      yield found shouldBe List(Person("1", "bob", 30))
    }
  }

  "getAll / getBy" should {
    "stream every document, and filtered documents" in {
      val repository = repo()
      val filter     = Field.of[Person, String](_.name).equalTo("alice")
      for
        _        <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        all      <- repository.getAll.compile.toList
        filtered <- repository.getBy(filter).compile.toList
      yield
        all should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25))
        filtered shouldBe List(Person("2", "alice", 25))
    }
  }

  "insertOne / insertMany" should {
    "add documents, batching inserts across round trips" in {
      val repository = repo(batchSize = 2)
      for
        _ <- repository.insertOne(Person("1", "bob", 30))
        _ <- repository.insertMany(List(Person("2", "alice", 25), Person("3", "eve", 40), Person("4", "carl", 22)))
      yield repository.fake.snapshot should contain theSameElementsAs
        List(Person("1", "bob", 30), Person("2", "alice", 25), Person("3", "eve", 40), Person("4", "carl", 22))
    }
  }

  "upsert / upsertMany" should {
    "insert when the key is new and replace when it already exists" in {
      val repository = repo()
      for
        _ <- repository.upsert(Person("1", "bob", 30))
        _ <- repository.upsert(Person("1", "bob", 31))
      yield repository.fake.snapshot shouldBe List(Person("1", "bob", 31))
    }

    "batch upserts across round trips" in {
      val repository = repo(batchSize = 1)
      repository
        .upsertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        .map(_ => repository.fake.snapshot should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25)))
    }
  }

  "updateField" should {
    "set a single field on the entity matching the key" in {
      val repository = repo()
      for
        _ <- repository.fake.insertOne(Person("1", "bob", 30))
        _ <- repository.updateField("1", Field.of[Person, Int](_.age), 31)
      yield repository.fake.snapshot shouldBe List(Person("1", "bob", 31))
    }
  }

  "updateBy" should {
    "apply an update to every document matching the filter and report the modified count" in {
      val repository = repo()
      for
        _        <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 30), Person("3", "eve", 40)))
        modified <- repository.updateBy(Field.of[Person, Int](_.age).equalTo(30), Update.set(Field.of[Person, Int](_.age), 99))
      yield
        modified.matchedCount shouldBe 2L
        repository.fake.snapshot should contain theSameElementsAs List(Person("1", "bob", 99), Person("2", "alice", 99), Person("3", "eve", 40))
    }
  }

  "bulkWrite" should {
    "apply mixed write commands, batching across round trips" in {
      val repository = repo(batchSize = 1)
      for
        _ <- repository.fake.insertOne(Person("1", "bob", 30))
        _ <- repository.bulkWrite(
               Seq(
                 WriteCommand.InsertOne(Person("2", "alice", 25)),
                 WriteCommand.DeleteOne(Field.of[Person, String](_.id).equalTo("1")),
               )
             )
      yield repository.fake.snapshot shouldBe List(Person("2", "alice", 25))
    }
  }

  "deleteOne / deleteMany" should {
    "remove by key, batching deletes across round trips" in {
      val repository = repo(batchSize = 1)
      for
        _ <- repository.fake.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3)))
        _ <- repository.deleteOne("1")
        _ <- repository.deleteMany(List("2", "3"))
      yield repository.fake.snapshot shouldBe empty
    }
  }

  "count" should {
    "report the total and filtered document count" in {
      val repository = repo()
      for
        _        <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        total    <- repository.count()
        filtered <- repository.count(Field.of[Person, Int](_.age).gt(26))
      yield
        total shouldBe 2L
        filtered shouldBe 1L
    }
  }
