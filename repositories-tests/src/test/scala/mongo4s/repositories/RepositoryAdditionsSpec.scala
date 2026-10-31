package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.testkit.FakeRepository
import mongo4s.operations.Update
import mongo4s.{Field, PrimaryKey}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

final class RepositoryAdditionsSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import BaseMongoRepositorySpec.Person
  import BaseMongoRepositorySpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def people(batchSize: Int = 500) = FakeRepository[IO, S, Person, String](batchSize)

  private val age  = Field.of[Person](_.age)
  private val name = Field.of[Person](_.name)

  "findOneByFilter" should {
    "answer one entity matching the filter, or none" in {
      val repository = people()

      for
        _       <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        found   <- repository.findOneByFilter(age.lt(28))
        missing <- repository.findOneByFilter(age.gt(90))
      yield
        found shouldBe Some(Person("2", "alice", 25))
        missing shouldBe None
    }
  }

  "estimatedCount" should {
    "count the collection" in {
      val repository = people()

      for
        _     <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        total <- repository.estimatedCount
      yield total shouldBe 2L
    }
  }

  "updateByKeys" should {
    "apply one update to every listed key, across batches" in {
      val repository = people(batchSize = 2)

      for
        _      <- repository.fake.insertMany((1 to 5).toList.map(i => Person(i.toString, s"p$i", 20)))
        result <- repository.updateByKeys(List("1", "3", "5", "3"), Update.set(age, 99))
      yield
        result.matchedCount shouldBe 3L
        repository.fake.snapshot.filter(_.age == 99).map(_.id) should contain theSameElementsAs List("1", "3", "5")
    }
  }

  "updateEach" should {
    "apply each key's own update" in {
      val repository = people(batchSize = 1)

      for
        _      <- repository.fake.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25)))
        result <- repository.updateEach(List("1" -> name.set("robert"), "2" -> age.set(26)))
      yield
        result.modifiedCount shouldBe 2L
        repository.fake.snapshot should contain theSameElementsAs List(Person("1", "robert", 30), Person("2", "alice", 26))
    }
  }

  "upsertKeeping" should {
    "overwrite every field but the ones kept, and insert what is missing without them" in {
      given PrimaryKey[BsonDocument, String] = PrimaryKey.storedId(_.getString("_id").getValue)

      val repository = FakeRepository[IO, S, BsonDocument, String]()
      val score      = Field.stored[BsonDocument, Int]("score")

      def doc(id: String, who: String, points: Int) = BsonDocument("_id", BsonString(id)).append("name", BsonString(who)).append("score", BsonInt32(points))

      for
        _ <- repository.fake.insertOne(doc("1", "bob", 5))
        _ <- repository.upsertKeeping(List(doc("1", "robert", 9), doc("2", "eve", 1)), List(score.path))
      yield repository.fake.snapshot should contain theSameElementsAs List(
        doc("1", "robert", 5),
        BsonDocument("_id", BsonString("2")).append("name", BsonString("eve")),
      )
    }

    "only make sure the key exists when every other field is kept" in {
      given PrimaryKey[BsonDocument, String] = PrimaryKey.storedId(_.getString("_id").getValue)

      val repository = FakeRepository[IO, S, BsonDocument, String]()
      val kept       = List(Field.stored[BsonDocument, String]("name").path)

      for
        _ <- repository.fake.insertOne(BsonDocument("_id", BsonString("1")).append("name", BsonString("bob")))
        _ <- repository.upsertKeeping(
               List(BsonDocument("_id", BsonString("1")).append("name", BsonString("x")), BsonDocument("_id", BsonString("2")).append("name", BsonString("y"))),
               kept,
             )
      yield repository.fake.snapshot should contain theSameElementsAs List(
        BsonDocument("_id", BsonString("1")).append("name", BsonString("bob")),
        BsonDocument("_id", BsonString("2")),
      )
    }
  }
