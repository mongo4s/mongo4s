package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class AggregationStagesItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val ageField = Field.of[Person, Int](_.age)

  private def seeded(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("stages_it")
      collection <- database.getCollection[Person](name)
      _          <- collection.insertMany(List(Person("alice", 5), Person("bob", 15), Person("carol", 25), Person("dave", 35)))
    yield collection

  "$bucket" should {
    "count documents into the boundaries it was given" in {
      val program =
        for
          collection <- seeded("bucket")
          buckets    <- collection
                          .aggregate[BsonDocument](
                            Seq(Stage.bucketBy(ageField, Seq(0, 20, 40), default = Some(BsonString("other")))("count" -> Accumulator.count[Person]))
                          )
                          .all
        yield buckets

      program.timeout(30.seconds).asserting { buckets =>
        buckets.map(b => b.get("_id") -> b.getNumber("count").intValue) shouldBe
          List(BsonInt32(0) -> 2, BsonInt32(20) -> 2)
      }
    }
  }

  "$densify" should {
    "fill in the steps no document was written for" in {
      val program =
        for
          collection <- seeded("densify")
          densified  <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.densify(ageField, DensifyRange.by(10).within(DensifyBounds.Between(BsonInt32(0), BsonInt32(40)))),
                              Stage.sortBy(Sort.asc(ageField)),
                            )
                          )
                          .all
        yield densified.map(_.getNumber("age").intValue)

      program.timeout(30.seconds).asserting { ages =>
        ages should contain allOf (0, 10, 20, 30)
        ages should contain allOf (5, 15, 25, 35)
      }
    }
  }

  "$setWindowFields" should {
    "compute a running total over the documents before each one" in {
      val program =
        for
          collection <- seeded("window")
          windowed   <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.setWindowFields(Sort.asc(ageField))(
                                "runningTotal" -> WindowOutput(Accumulator.sum(ageField))
                                  .over(Window.documents(WindowBound.Unbounded, WindowBound.Current))
                              ),
                              Stage.sortBy(Sort.asc(ageField)),
                            )
                          )
                          .all
        yield windowed.map(_.getNumber("runningTotal").intValue)

      program.timeout(30.seconds).asserting(_ shouldBe List(5, 20, 45, 80))
    }
  }
