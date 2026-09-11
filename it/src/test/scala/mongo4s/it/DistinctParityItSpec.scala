package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.Filter
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class DistinctParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val city   = Field.stored[BsonDocument, String]("city")
  private val age    = Field.stored[BsonDocument, Int]("age")
  private val labels = Field.stored[BsonDocument, String]("labels")

  private def person(name: String, town: String, years: Int, tags: List[String]): BsonDocument =
    BsonDocument("name", BsonString(name))
      .append("city", BsonString(town))
      .append("age", BsonInt32(years))
      .append("labels", BsonArray(tags.map(t => BsonString(t): BsonValue).asJava))

  private val people = List(
    person("bob", "berlin", 30, List("a", "b")),
    person("alice", "berlin", 25, List("b", "c")),
    person("carol", "prague", 30, List("c")),
  )

  private def real(name: String): IO[MongoCollection[IO, S, BsonDocument]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("distinct_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertMany(people)
    yield collection

  private def fake: IO[FakeMongoCollection[IO, S, BsonDocument]] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]], _ => fs2.Stream.empty)
    collection.insertMany(people).as(collection)

  private def parity[B: Ordering](
      name: String,
      read: MongoCollection[IO, S, BsonDocument] => IO[List[B]],
  ): IO[(List[B], List[B])] =
    for
      collection <- real(name)
      simulated  <- fake
      fromServer <- read(collection)
      fromFake   <- read(simulated)
    yield (fromServer.sorted, fromFake.sorted)

  "the fake's distinct" should {

    "return the same values as the server for a scalar field" in {
      parity[String]("scalar", _.distinct(city).all)
        .timeout(30.seconds)
        .asserting { (fromServer, fromFake) =>
          fromFake shouldBe fromServer
          fromFake shouldBe List("berlin", "prague")
        }
    }

    "collapse repeats the same way for a numeric field" in {
      parity[Int]("numeric", _.distinct(age).all)
        .timeout(30.seconds)
        .asserting { (fromServer, fromFake) =>
          fromFake shouldBe fromServer
          fromFake shouldBe List(25, 30)
        }
    }

    "flatten an array field into its elements, as the server does" in {
      parity[String]("array", _.distinct(labels).all)
        .timeout(30.seconds)
        .asserting { (fromServer, fromFake) =>
          fromFake shouldBe fromServer
          fromFake shouldBe List("a", "b", "c")
        }
    }

    "apply the filter before collecting the values" in {
      parity[String]("filtered", _.distinct(city, age.gt(28)).all)
        .timeout(30.seconds)
        .asserting { (fromServer, fromFake) =>
          fromFake shouldBe fromServer
          fromFake shouldBe List("berlin", "prague")
        }
    }

    "report nothing when the filter matches nothing" in {
      parity[String]("empty", _.distinct(city, Filter.none[BsonDocument]).all)
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => (fromFake, fromServer) shouldBe (Nil, Nil))
    }
  }
