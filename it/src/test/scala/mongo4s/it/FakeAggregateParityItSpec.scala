package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, BsonError}
import mongo4s.cats.CatsStream
import mongo4s.operations.{Accumulator, Projection, Sort, Stage}
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object FakeAggregateParityItSpec:

  final case class Person(id: String, name: String, age: Int, city: String)

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person =>
        BsonDocument()
          .append("id", BsonString(person.id))
          .append("name", BsonString(person.name))
          .append("age", BsonInt32(person.age))
          .append("city", BsonString(person.city)),
      document =>
        for
          id   <- field[String](document, "id")
          name <- field[String](document, "name")
          age  <- field[Int](document, "age")
          city <- field[String](document, "city")
        yield Person(id, name, age, city),
    )

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

  val people: List[Person] = List(
    Person("1", "bob", 30, "berlin"),
    Person("2", "alice", 25, "berlin"),
    Person("3", "carol", 41, "prague"),
    Person("4", "dave", 30, "prague"),
  )

final class FakeAggregateParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import FakeAggregateParityItSpec.*
  import FakeAggregateParityItSpec.Person.given

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val age  = Field.of[Person, Int](_.age)
  private val city = Field.of[Person, String](_.city)
  private val name = Field.of[Person, String](_.name)

  private val byId: Stage[Person] = Stage.sortBy(Sort.asc(Field.stored[Person, String]("_id")))

  private def real(collectionName: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_parity_it")
      collection <- database.getCollection[Person](collectionName)
      _          <- collection.insertMany(people)
    yield collection

  private def fake: FakeMongoCollection[IO, S, Person] =
    val collection = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]])
    collection.insertMany(people).unsafeRunSync()
    collection

  private def withoutStoredId(document: BsonDocument): BsonDocument =
    val copy = BsonDocument()
    document.entrySet.forEach(entry => if entry.getKey != "_id" || !entry.getValue.isObjectId then copy.append(entry.getKey, entry.getValue): Unit)
    copy

  private def parity(collectionName: String, stages: Stage[Person]*): IO[(List[BsonDocument], List[BsonDocument])] =
    for
      collection <- real(collectionName)
      fromServer <- collection.aggregate[BsonDocument](stages).all
      fromFake   <- fake.aggregate[BsonDocument](stages).all
    yield (fromServer.map(withoutStoredId), fromFake.map(withoutStoredId))

  "the fake's aggregate" should {

    "match, sort, skip and limit exactly as the server does" in {
      parity("match_sort", Stage.matching(age.gte(30)), Stage.sortBy(Sort.desc(age).asc(name)), Stage.skip(1), Stage.limit(2))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $count, including its BSON type" in {
      parity("count", Stage.matching(city.equalTo("berlin")), Stage.count("total"))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $group counts, sums and averages, including their BSON types" in {
      parity(
        "group",
        Stage.groupBy(city)(
          "people" -> Accumulator.count[Person],
          "years"  -> Accumulator.sum(age),
          "mean"   -> Accumulator.avg(age),
          "oldest" -> Accumulator.max(age),
          "first"  -> Accumulator.first(name),
        ),
        byId,
      ).timeout(30.seconds).asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $project output" in {
      parity("project", Stage.project(Projection.empty[Person].include(name).include(age)), Stage.sortBy(Sort.asc(name)))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $sortByCount counts, including their BSON type" in {
      val count = Field.stored[Person, Int]("count")

      parity("sort_by_count", Stage.sortByCount(age), Stage.sortBy(Sort.desc(count).asc(Field.stored[Person, Int]("_id"))))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $firstN, $lastN, $maxN and $minN, an n past the group's size included" in {
      parity(
        "n_accumulators",
        Stage.sortBy(Sort.asc(Field.stored[Person, String]("id"))),
        Stage.groupBy(city)(
          "firstTwo" -> Accumulator.firstN(name, 2),
          "lastOne"  -> Accumulator.lastN(name, 1),
          "oldest"   -> Accumulator.maxN(age, 5),
          "youngest" -> Accumulator.minN(age, 1),
        ),
        byId,
      ).timeout(30.seconds).asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same $top, $bottom, $topN and $bottomN" in {
      parity(
        "ranked_accumulators",
        Stage.groupBy(city)(
          "eldest"   -> Accumulator.top(Sort.desc(age), name),
          "youngest" -> Accumulator.bottom(Sort.desc(age), name),
          "byName"   -> Accumulator.topN(Sort.asc(name), age, 5),
          "lastName" -> Accumulator.bottomN(Sort.asc(name), name, 1),
        ),
        byId,
      ).timeout(30.seconds).asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }

    "produce the same standard deviations, to the last few bits" in {
      parity("std_dev", Stage.groupAll[Person]("pop" -> Accumulator.stdDevPop(age), "samp" -> Accumulator.stdDevSamp(age)))
        .timeout(30.seconds)
        .asserting { (fromServer, fromFake) =>
          fromFake.map(_.getDouble("pop").getValue).head shouldBe fromServer.map(_.getDouble("pop").getValue).head +- 1e-9
          fromFake.map(_.getDouble("samp").getValue).head shouldBe fromServer.map(_.getDouble("samp").getValue).head +- 1e-9
        }
    }

    "produce the same $unset output" in {
      parity("unset", Stage.unset(age).and(city), Stage.sortBy(Sort.asc(name)))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => fromFake shouldBe fromServer)
    }
  }
