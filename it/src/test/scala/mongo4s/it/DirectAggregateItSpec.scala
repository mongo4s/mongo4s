package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt64, BsonString}

import mongo4s.bson.direct.WireCodec
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, BsonError}
import mongo4s.cats.CatsStream
import mongo4s.operations.{Accumulator, Stage}
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object DirectAggregateItSpec:
  final case class Person(name: String, age: Int) derives WireCodec

  final case class ByAge(_id: Int, total: Int) derives WireCodec

  final case class Counted(count: Long, label: String) derives WireCodec

  val lenientCounted: BsonDocumentCodec[Counted] = BsonDocumentCodec.make(
    counted => BsonDocument("count", BsonInt64(counted.count)).append("label", BsonString(counted.label)),
    document =>
      Option(document.get("count"))
        .toRight(BsonError.MissingField("count"))
        .flatMap(BsonDecoder[Long].decode)
        .map(count => Counted(count, Option(document.get("label")).fold("total")(_.asString.getValue))),
  )

final class DirectAggregateItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import DirectAggregateItSpec.*

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def seeded(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("direct_aggregate_it")
      collection <- database.getDirectCollection[Person](name)
      _          <- collection.insertMany(List(Person("bob", 30), Person("alice", 30), Person("carol", 25)))
    yield collection

  "aggregateDirect" should {

    "decode the pipeline's output through its WireCodec" in {
      val program =
        for
          collection <- seeded("grouped")
          grouped    <- collection
                          .aggregateDirect[ByAge](
                            Seq(
                              Stage.groupBy(Field.of[Person, Int](_.age))("total" -> Accumulator.count[Person]),
                              Stage.raw[Person](BsonDocument("$sort", BsonDocument("_id", org.bson.BsonInt32(1)))),
                            )
                          )
                          .all
        yield grouped

      program.timeout(30.seconds).asserting(_ shouldBe List(ByAge(25, 1), ByAge(30, 2)))
    }

    "keep the options the query was built with" in {
      val program =
        for
          collection <- seeded("options")
          first      <- collection
                          .aggregateDirect[ByAge](
                            Seq(Stage.groupBy(Field.of[Person, Int](_.age))("total" -> Accumulator.count[Person]))
                          )
                          .allowDiskUse(true)
                          .comment("direct aggregate")
                          .batchSize(16)
                          .first
        yield first

      program.timeout(30.seconds).asserting(_.map(_.total) should not be empty)
    }

    "read the pipeline's output with the strictness a WireCodec has, not a document codec's" in {
      val stages = Seq(Stage.count[Person]("count"))

      val program =
        for
          collection <- seeded("strictness")
          lenient    <- collection.aggregate[Counted](stages)(using None)(using lenientCounted).first
          strict     <- collection.aggregateDirect[Counted](stages).first.attempt
        yield (lenient, strict)

      program.timeout(30.seconds).asserting { (lenient, strict) =>
        lenient shouldBe Some(Counted(3L, "total"))
        strict.isLeft shouldBe true
      }
    }

    "report a document it cannot decode through attempting, rather than failing the query" in {
      val stages = Seq(Stage.count[Person]("count"))

      val program =
        for
          collection <- seeded("attempting")
          attempts   <- collection.aggregateDirect[Counted](stages).attempting.all
        yield attempts

      program.timeout(30.seconds).asserting { attempts =>
        attempts.size shouldBe 1
        attempts.head.isLeft shouldBe true
      }
    }
  }
