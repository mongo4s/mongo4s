package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO

import mongo4s.Field
import mongo4s.cats.CatsStream
import mongo4s.bson.BsonDocumentCodec
import mongo4s.operations.{Filter, Projection, Update, WriteCommand}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

final class BatchingSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import BaseMongoRepositorySpec.Person
  import BaseMongoRepositorySpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def repo(
      batchSize: Int,
      projection: Projection[Person] = Projection.empty[Person],
  ): (FakeMongoCollection[IO, S, Person], BaseMongoRepository[IO, S, Person, String]) =
    val collection = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]], list => fs2.Stream.emits(list))
    (collection, BaseMongoRepository(collection, batchSize, projection))

  private def people(n: Int): List[Person] = (1 to n).toList.map(i => Person(i.toString, s"p$i", 20 + i))

  "upsertMany across batches" should {
    "report one upserted id per entity, keyed by its position in the whole list" in {
      val (_, repository) = repo(batchSize = 2)

      repository.upsertMany(people(5)).asserting { result =>
        result.upsertedIds.keySet shouldBe Set(0, 1, 2, 3, 4)
        result.upsertedIds.values.toSet should have size 5
      }
    }

    "still write every entity" in {
      val (collection, repository) = repo(batchSize = 2)

      repository.upsertMany(people(5)) *> IO(collection.snapshot.map(_.id).sorted shouldBe List("1", "2", "3", "4", "5"))
    }

    "sum the counts of an update-only bulk write" in {
      val (collection, repository) = repo(batchSize = 2)
      val commands                 = people(5).map(p => WriteCommand.updateOne(Field.of[Person, String](_.id).equalTo(p.id), Update.set(Field.of[Person, Int](_.age), 99)))

      for
        _      <- collection.insertMany(people(5))
        result <- repository.bulkWrite(commands)
      yield result.modifiedCount shouldBe 5L
    }
  }

  "findMany and deleteMany across batches" should {
    "cover every key" in {
      val (collection, repository) = repo(batchSize = 2)

      for
        _       <- collection.insertMany(people(5))
        found   <- repository.findMany(people(5).map(_.id))
        deleted <- repository.deleteMany(List("1", "2", "3"))
        left    <- repository.count()
      yield
        found should have size 5
        deleted.deletedCount shouldBe 3L
        left shouldBe 2L
    }

    "issue no query at all for an empty key list" in {
      val (_, repository) = repo(batchSize = 2)

      for
        found   <- repository.findMany(Nil)
        deleted <- repository.deleteMany(Nil)
      yield
        found shouldBe Nil
        deleted.deletedCount shouldBe 0L
    }
  }

  "the repository's default projection" should {
    "apply to findOneAndUpdate as well as to finds" in {
      val (collection, repository) = repo(batchSize = 500, projection = Projection.empty[Person].include(Field.of[Person, String](_.id)))

      for
        _       <- collection.insertOne(Person("1", "bob", 30))
        updated <- repository.findOneAndUpdate("1", Update.set(Field.of[Person, Int](_.age), 31))
      yield
        updated shouldBe None
        collection.snapshot shouldBe List(Person("1", "bob", 31))
    }

    "return the whole entity when the projection is empty" in {
      val (collection, repository) = repo(batchSize = 500)

      for
        _       <- collection.insertOne(Person("1", "bob", 30))
        updated <- repository.findOneAndUpdate("1", Update.set(Field.of[Person, Int](_.age), 31))
      yield updated shouldBe Some(Person("1", "bob", 31))
    }
  }

  "count" should {
    "accept an explicit filter" in {
      val (collection, repository) = repo(batchSize = 500)

      for
        _     <- collection.insertMany(people(5))
        adult <- repository.count(Field.of[Person, Int](_.age).gte(24))
        all   <- repository.count(Filter.all)
      yield
        adult shouldBe 2L
        all shouldBe 5L
    }
  }
