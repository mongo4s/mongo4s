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

  "$unset" should {
    "drop the fields it names from every document" in {
      val program =
        for
          collection <- seeded("unset")
          stripped   <- collection.aggregate[BsonDocument](Seq(Stage.unset(ageField).and(Field.stored[Person, Any]("_id")))).all
        yield stripped.map(_.keySet)

      program.timeout(30.seconds).asserting(_.distinct shouldBe List(java.util.Set.of("name")))
    }
  }

  "$bucketAuto" should {
    "split the documents evenly into the number of buckets asked for" in {
      val program =
        for
          collection <- seeded("bucket_auto")
          buckets    <- collection.aggregate[BsonDocument](Seq(Stage.bucketAuto(ageField, 2)("count" -> Accumulator.count[Person]))).all
        yield buckets.map(b => (b.getDocument("_id").getNumber("min").intValue, b.getDocument("_id").getNumber("max").intValue, b.getNumber("count").intValue))

      program.timeout(30.seconds).asserting(_ shouldBe List((5, 25, 2), (25, 35, 2)))
    }

    "round the boundaries to the series the granularity names" in {
      val program =
        for
          collection <- seeded("bucket_auto_rounded")
          buckets    <- collection.aggregate[BsonDocument](Seq(Stage.bucketAutoRounded(ageField, 2, BucketGranularity.R5)())).all
        yield buckets.map(b => (b.getDocument("_id").getNumber("min").doubleValue, b.getDocument("_id").getNumber("max").doubleValue))

      program.timeout(30.seconds).asserting(_ shouldBe List((4.0, 16.0), (16.0, 40.0)))
    }
  }

  "$median and $percentile" should {
    "answer from the values they were given, the percentiles in the order asked" in {
      val program =
        for
          collection <- seeded("percentiles")
          found      <- collection
                          .aggregate[BsonDocument](
                            Seq(Stage.groupAll[Person]("median" -> Accumulator.median(ageField), "spread" -> Accumulator.percentile(ageField, 0.1, 0.9)))
                          )
                          .all
        yield found.map(document => (document.getNumber("median").doubleValue, document.getArray("spread").getValues.toArray.toList))

      program.timeout(30.seconds).asserting { found =>
        val (median, spread) = found.head
        median should (be >= 15.0 and be <= 25.0)
        spread.map(_.asInstanceOf[org.bson.BsonValue].asNumber.doubleValue) shouldBe List(5.0, 35.0)
      }
    }
  }

  "$sortByCount" should {
    "count each value and put the most frequent first" in {
      val program =
        for
          collection <- seeded("sort_by_count")
          _          <- collection.insertMany(List(Person("erin", 15), Person("finn", 15), Person("gus", 25)))
          counted    <- collection.aggregate[BsonDocument](Seq(Stage.sortByCount(ageField))).all
        yield counted.map(c => c.getNumber("_id").intValue -> c.getNumber("count").intValue)

      program.timeout(30.seconds).asserting(_.take(2) shouldBe List(15 -> 3, 25 -> 2))
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
