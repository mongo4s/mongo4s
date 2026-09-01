package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.Filter
import mongo4s.testkit.FakeMongoCollection

import mongo4s.cats.CatsInstances.given

final class DecodeAttemptsSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import BaseMongoRepositorySpec.Person
  import BaseMongoRepositorySpec.Person.given

  private type S[A] = CatsStream[IO][A]

  private val corrupt =
    BsonDocument()
      .append("id", BsonString("2"))
      .append("name", BsonString("broken"))
      .append("age", BsonString("not-a-number"))

  private def withOneCorruptDocument: IO[FakeMongoCollection[IO, S, Person]] =
    val fake = FakeMongoCollection[IO, S, Person](
      summon[BsonDocumentCodec[Person]],
      values => fs2.Stream.emits(values).covary[IO],
      values => fs2.Stream.emits(values).covary[IO],
    )

    for
      _ <- fake.insertOne(Person("1", "alice", 30))
      _ <- IO(fake.insertRaw(corrupt))
      _ <- fake.insertOne(Person("3", "carol", 40))
    yield fake

  "all" should {
    "fail the whole effect when a document does not decode" in {
      withOneCorruptDocument.flatMap(_.find().all.attempt).asserting(_.isLeft shouldBe true)
    }
  }

  "attempting.all" should {
    "return every document, with failures as Left and the good ones intact" in {
      withOneCorruptDocument.flatMap(_.find().attempting.all).asserting { attempts =>
        attempts.size shouldBe 3
        attempts.count(_.isLeft) shouldBe 1
        attempts.collect { case Right(person) => person.id } shouldBe List("1", "3")
      }
    }

    "carry the structured error rather than only a message" in {
      withOneCorruptDocument.flatMap(_.find().attempting.all).asserting { attempts =>
        attempts.collect { case Left(error: BsonError.TypeMismatch) => error } should have size 1
      }
    }

    "honour the query's filter, skip and limit" in {
      withOneCorruptDocument.flatMap(_.find(Filter.all[Person]).limit(2).attempting.all).asserting(_.size shouldBe 2)
    }
  }

  "attempting.stream" should {
    "emit failures alongside successes rather than terminating" in {
      withOneCorruptDocument.flatMap(_.find().attempting.stream.compile.toList).asserting { attempts =>
        attempts.size shouldBe 3
        attempts.count(_.isLeft) shouldBe 1
      }
    }
  }
