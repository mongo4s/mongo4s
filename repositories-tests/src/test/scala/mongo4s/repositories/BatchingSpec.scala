package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.testkit.FakeRepository
import mongo4s.{Field, MongoError, PrimaryKey}
import mongo4s.operations.{Filter, FindOneAndUpdateOptions, Projection, ReplaceOptions, Update, WriteCommand}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

final class BatchingSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import BaseMongoRepositorySpec.Person
  import BaseMongoRepositorySpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def repo(
      batchSize: Int,
      projection: Projection[Person] = Projection.empty[Person],
  ): FakeRepository[IO, S, Person, String] =
    FakeRepository[IO, S, Person, String](list => fs2.Stream.emits(list), batchSize, projection)

  private def people(n: Int): List[Person] = (1 to n).toList.map(i => Person(i.toString, s"p$i", 20 + i))

  private given PrimaryKey[BsonDocument, String] = PrimaryKey.storedId(_.getString("_id").getValue)

  private def keyed(ids: String*): List[BsonDocument] = ids.toList.map(id => BsonDocument("_id", BsonString(id)))

  private def documents(batchSize: Int): FakeRepository[IO, S, BsonDocument, String] =
    FakeRepository[IO, S, BsonDocument, String](list => fs2.Stream.emits(list), batchSize)

  "a batch that fails after earlier batches were written" should {
    "report the failing command by its position in the whole bulk write, with what the earlier batches wrote" in {
      val repository = documents(batchSize = 2)

      repository.bulkWrite(keyed("a", "b", "a").map(WriteCommand.InsertOne(_))).attempt.asserting {
        case Left(failed: MongoError.BulkWriteFailed) =>
          failed.failures.map(_.index) shouldBe List(2)
          failed.result.insertedCount shouldBe 2
        case other                                    => fail(s"expected a BulkWriteFailed, got $other")
      }
    }

    "report the failing entity of an insertMany by its position in the whole list" in {
      val repository = documents(batchSize = 2)

      repository.insertMany(keyed("a", "b", "c", "d", "a")).attempt.asserting {
        case Left(failed: MongoError.BulkWriteFailed) =>
          failed.failures.map(_.index) shouldBe List(4)
          failed.result.insertedCount shouldBe 4
        case other                                    => fail(s"expected a BulkWriteFailed, got $other")
      }
    }

    "keep the upserted ids of the batches that went through" in {
      val repository = documents(batchSize = 2)

      repository.fake.insertOne(BsonDocument("_id", BsonString("z"))) *>
        repository
          .bulkWrite(
            List(
              WriteCommand.ReplaceOne(Field.stored[BsonDocument, String]("_id").equalTo("a"), keyed("a").head, ReplaceOptions.upsert),
              WriteCommand.ReplaceOne(Field.stored[BsonDocument, String]("_id").equalTo("b"), keyed("b").head, ReplaceOptions.upsert),
              WriteCommand.InsertOne(keyed("z").head),
            )
          )
          .attempt
          .asserting {
            case Left(failed: MongoError.BulkWriteFailed) =>
              failed.result.upsertedIds.keySet shouldBe Set(0, 1)
              failed.failures.map(_.index) shouldBe List(2)
            case other                                    => fail(s"expected a BulkWriteFailed, got $other")
          }
    }
  }

  "upsertMany across batches" should {
    "report one upserted id per entity, keyed by its position in the whole list" in {
      val repository = repo(batchSize = 2)

      repository.upsertMany(people(5)).asserting { result =>
        result.upsertedIds.keySet shouldBe Set(0, 1, 2, 3, 4)
        result.upsertedIds.values.toSet should have size 5
      }
    }

    "still write every entity" in {
      val repository = repo(batchSize = 2)

      repository.upsertMany(people(5)) *> IO(repository.fake.snapshot.map(_.id).sorted shouldBe List("1", "2", "3", "4", "5"))
    }

    "sum the counts of an update-only bulk write" in {
      val repository = repo(batchSize = 2)
      val commands   = people(5).map(p => WriteCommand.updateOne(Field.of[Person, String](_.id).equalTo(p.id), Update.set(Field.of[Person, Int](_.age), 99)))

      for
        _      <- repository.fake.insertMany(people(5))
        result <- repository.bulkWrite(commands)
      yield result.modifiedCount shouldBe 5L
    }
  }

  "findMany and deleteMany across batches" should {
    "cover every key" in {
      val repository = repo(batchSize = 2)

      for
        _       <- repository.fake.insertMany(people(5))
        found   <- repository.findMany(people(5).map(_.id))
        deleted <- repository.deleteMany(List("1", "2", "3"))
        left    <- repository.count()
      yield
        found should have size 5
        deleted.deletedCount shouldBe 3L
        left shouldBe 2L
    }

    "issue no query at all for an empty key list" in {
      val repository = repo(batchSize = 2)

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
      val repository = repo(batchSize = 500, projection = Projection.empty[Person].include(Field.of[Person, String](_.id)))

      for
        _       <- repository.fake.insertOne(Person("1", "bob", 30))
        updated <- repository.findOneAndUpdate("1", Update.set(Field.of[Person, Int](_.age), 31))
      yield
        updated shouldBe None
        repository.fake.snapshot shouldBe List(Person("1", "bob", 31))
    }

    "return the whole entity when the projection is empty" in {
      val repository = repo(batchSize = 500)

      for
        _       <- repository.fake.insertOne(Person("1", "bob", 30))
        updated <- repository.findOneAndUpdate("1", Update.set(Field.of[Person, Int](_.age), 31))
      yield updated shouldBe Some(Person("1", "bob", 31))
    }

    "give way to a projection the caller passed to findOneAndUpdate" in {
      val repository = repo(batchSize = 500, projection = Projection.empty[Person].exclude(Field.of[Person, Int](_.age)))
      val everything =
        Projection.empty[Person].include(Field.of[Person, String](_.id)).include(Field.of[Person, String](_.name)).include(Field.of[Person, Int](_.age))

      for
        _       <- repository.fake.insertOne(Person("1", "bob", 30))
        updated <- repository.findOneAndUpdate("1", Update.set(Field.of[Person, Int](_.age), 31), FindOneAndUpdateOptions.default[Person].withProjection(everything))
      yield updated shouldBe Some(Person("1", "bob", 31))
    }
  }

  "count" should {
    "accept an explicit filter" in {
      val repository = repo(batchSize = 500)

      for
        _     <- repository.fake.insertMany(people(5))
        adult <- repository.count(Field.of[Person, Int](_.age).gte(24))
        all   <- repository.count(Filter.all)
      yield
        adult shouldBe 2L
        all shouldBe 5L
    }
  }
