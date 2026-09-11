package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonType}

import mongo4s.bson.direct.WireCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.Update
import mongo4s.repositories.BaseMongoRepository
import mongo4s.{Field, MongoClient, MongoCollection, PrimaryKey}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object NumericUpdateItSpec:

  final case class Person(id: String, age: Int) derives WireCodec

  object Person:
    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

final class NumericUpdateItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import NumericUpdateItSpec.Person
  import NumericUpdateItSpec.Person.given

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val age = Field.of[Person, Int](_.age)

  private def seeded(name: String): IO[(BaseMongoRepository[IO, S, Person, String], MongoCollection[IO, S, BsonDocument])] =
    for
      client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database <- client.getDatabase("numeric_update_it")
      typed    <- database.getDirectCollection[Person](name)
      raw      <- database.getCollection[BsonDocument](name)
      _        <- typed.insertOne(Person("a", 30))
    yield (BaseMongoRepository(typed), raw)

  private def storedAge(raw: MongoCollection[IO, S, BsonDocument]): IO[(BsonType, Long)] =
    raw.find().first.map { document =>
      val value = document.get.get("age")
      (value.getBsonType, value.asNumber.longValue)
    }

  "an update over an Int field" should {

    "write the width the model declares, not a wider one" in {
      val program =
        for
          (repo, raw) <- seeded("set")
          _           <- repo.updateField("a", age, 31)
          stored      <- storedAge(raw)
        yield stored

      program.timeout(30.seconds).asserting(_ shouldBe (BsonType.INT32, 31L))
    }

    "keep that width through $inc and $mul" in {
      val program =
        for
          (repo, raw) <- seeded("inc_mul")
          _           <- repo.updateOne("a", Update.inc(age, 1))
          _           <- repo.updateOne("a", Update.mul(age, 2))
          stored      <- storedAge(raw)
        yield stored

      program.timeout(30.seconds).asserting(_ shouldBe (BsonType.INT32, 62L))
    }

    "widen where the server widens it — an $inc that overflows an Int32 is stored as an Int64" in {
      val program =
        for
          (repo, raw) <- seeded("overflow")
          _           <- repo.updateOne("a", Update.inc(age, Int.MaxValue))
          stored      <- storedAge(raw)
        yield stored

      program.timeout(30.seconds).asserting(_ shouldBe (BsonType.INT64, 30L + Int.MaxValue))
    }

    "report that overflow when reading, naming the value rather than the BSON type" in {
      val program =
        for
          (repo, _) <- seeded("overflow_read")
          _         <- repo.updateOne("a", Update.inc(age, Int.MaxValue))
          read      <- repo.findOne("a").attempt
        yield read

      program.timeout(30.seconds).asserting {
        case Left(error)  => error.getMessage should include("out of range for Int")
        case Right(value) => fail(s"expected a decoding failure, got $value")
      }
    }

    "read a widened value back when it still fits the model" in {
      val program =
        for
          (repo, raw) <- seeded("widened_but_small")
          _           <- raw.updateOne(
                           Field.stored[BsonDocument, String]("id").equalTo("a"),
                           Update.Raw(BsonDocument("$set", BsonDocument("age", org.bson.BsonInt64(31L)))),
                         )
          stored      <- storedAge(raw)
          read        <- repo.findOne("a")
        yield (stored._1, read)

      program.timeout(30.seconds).asserting(_ shouldBe (BsonType.INT64, Some(Person("a", 31))))
    }
  }
