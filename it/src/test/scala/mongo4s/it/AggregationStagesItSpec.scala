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

  "window functions" should {
    val t = Field.stored[BsonDocument, Int]("t")
    val v = Field.stored[BsonDocument, Int]("v")

    def series(name: String, points: (Int, Option[Int])*): IO[MongoCollection[IO, S, BsonDocument]] =
      for
        client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
        database   <- client.getDatabase("stages_it")
        collection <- database.getCollection[BsonDocument](name)
        _          <- collection.insertMany(points.toList.map { (at, value) =>
                        BsonDocument("t", BsonInt32(at)).append("v", value.fold(org.bson.BsonNull.VALUE: org.bson.BsonValue)(BsonInt32(_)))
                      })
      yield collection

    def windowed(collection: MongoCollection[IO, S, BsonDocument], output: (String, WindowField[BsonDocument])*): IO[List[BsonDocument]] =
      collection.aggregate[BsonDocument](Seq(Stage.setWindowFields(Sort.asc(t))(output*), Stage.sortBy(Sort.asc(t)))).all

    def numbers(documents: List[BsonDocument], name: String): List[Option[Double]] =
      documents.map(document => Option(document.get(name)).filter(_.isNumber).map(_.asNumber.doubleValue))

    "rank, rank densely and number the documents, ties shared where the server shares them" in {
      val program =
        for
          collection <- series("ranks", 1 -> Some(5), 2 -> Some(15), 2 -> Some(15), 3 -> Some(25))
          ranked     <- windowed(collection, "rank" -> WindowFunction.rank[BsonDocument], "dense" -> WindowFunction.denseRank[BsonDocument])
          numbered   <- windowed(collection, "row" -> WindowFunction.documentNumber[BsonDocument])
        yield (numbers(ranked, "rank"), numbers(ranked, "dense"), numbers(numbered, "row"))

      program.timeout(30.seconds).asserting { (rank, dense, row) =>
        rank.flatten shouldBe List(1.0, 2.0, 2.0, 4.0)
        dense.flatten shouldBe List(1.0, 2.0, 2.0, 3.0)
        row.flatten.sorted shouldBe List(1.0, 2.0, 3.0, 4.0)
      }
    }

    "shift a value from a neighbour, with the default where there is none" in {
      val program =
        for
          collection <- series("shift", 1 -> Some(10), 2 -> Some(20), 3 -> Some(40))
          shifted    <- windowed(collection, "prev" -> WindowFunction.shift(v, by = -1, default = Some(0)))
        yield numbers(shifted, "prev")

      program.timeout(30.seconds).asserting(_ shouldBe List(Some(0.0), Some(10.0), Some(20.0)))
    }

    "fill a gap with the last value, or with the line between its neighbours" in {
      val program =
        for
          collection <- series("fill", 1 -> Some(10), 2 -> None, 3 -> Some(30))
          filled     <- windowed(collection, "kept" -> WindowFunction.locf(v), "line" -> WindowFunction.linearFill(v))
        yield (numbers(filled, "kept"), numbers(filled, "line"))

      program.timeout(30.seconds).asserting { (kept, line) =>
        kept shouldBe List(Some(10.0), Some(10.0), Some(30.0))
        line shouldBe List(Some(10.0), Some(20.0), Some(30.0))
      }
    }

    "differentiate and integrate over the window they were given" in {
      val program =
        for
          collection <- series("calculus", 1 -> Some(10), 2 -> Some(20), 3 -> Some(40))
          computed   <- windowed(
                          collection,
                          "slope" -> WindowFunction.derivative(v, Window.documents(WindowBound.At(-1), WindowBound.Current)),
                          "area"  -> WindowFunction.integral(v, Window.documents(WindowBound.Unbounded, WindowBound.Current)),
                        )
        yield (numbers(computed, "slope"), numbers(computed, "area"))

      program.timeout(30.seconds).asserting { (slope, area) =>
        slope.drop(1) shouldBe List(Some(10.0), Some(20.0))
        area shouldBe List(Some(0.0), Some(15.0), Some(45.0))
      }
    }

    "average exponentially, by a count of documents or by alpha" in {
      val program =
        for
          collection <- series("ema", 1 -> Some(10), 2 -> Some(20), 3 -> Some(40))
          averaged   <- windowed(collection, "byN" -> WindowFunction.expMovingAvg(v, 2), "byAlpha" -> WindowFunction.expMovingAvgAlpha(v, 0.5))
        yield (numbers(averaged, "byN").flatten, numbers(averaged, "byAlpha").flatten)

      program.timeout(30.seconds).asserting { (byN, byAlpha) =>
        byN.zip(List(10.0, 50.0 / 3, 290.0 / 9)).foreach((actual, expected) => actual shouldBe expected +- 1e-9)
        byAlpha shouldBe List(10.0, 15.0, 27.5)
      }
    }
  }

  "$redact" should {
    "prune what its expression says to, and descend into the rest" in {
      val levelled = Seq(
        Stage.redact[BsonDocument](
          BsonDocument(
            "$cond",
            BsonDocument("if", BsonDocument("$eq", org.bson.BsonArray(java.util.List.of(BsonString("$level"), BsonInt32(1)))))
              .append("then", Stage.Prune)
              .append("else", Stage.Descend),
          )
        )
      )

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("stages_it")
          collection <- database.getCollection[BsonDocument]("redact")
          _          <- collection.insertMany(
                          List(
                            BsonDocument("name", BsonString("open"))
                              .append("level", BsonInt32(0))
                              .append("secret", BsonDocument("level", BsonInt32(1)).append("pin", BsonInt32(1234))),
                            BsonDocument("name", BsonString("hidden")).append("level", BsonInt32(1)),
                          )
                        )
          kept       <- collection.aggregate[BsonDocument](levelled).all
        yield kept.map(document => (document.getString("name").getValue, document.containsKey("secret")))

      program.timeout(30.seconds).asserting(_ shouldBe List("open" -> false))
    }
  }

  "$lookup joined on keys with a sub-pipeline" should {
    "match by key and then narrow the joined documents with the pipeline" in {
      val owner  = Field.stored[BsonDocument, String]("owner")
      val id     = Field.stored[BsonDocument, String]("id")
      val amount = Field.stored[BsonDocument, Int]("amount")

      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("stages_it")
          users    <- database.getCollection[BsonDocument]("keyed_users")
          orders   <- database.getCollection[BsonDocument]("keyed_orders")
          _        <- users.insertMany(List(BsonDocument("id", BsonString("u1")), BsonDocument("id", BsonString("u2"))))
          _        <- orders.insertMany(
                        List(
                          BsonDocument("owner", BsonString("u1")).append("amount", BsonInt32(5)),
                          BsonDocument("owner", BsonString("u1")).append("amount", BsonInt32(50)),
                          BsonDocument("owner", BsonString("u2")).append("amount", BsonInt32(70)),
                        )
                      )
          joined   <- users
                        .aggregate[BsonDocument](
                          Seq(
                            Stage.lookupWithKeys[BsonDocument, BsonDocument, String]("keyed_orders", id, owner, List(Stage.matching(amount.gte(10))), "big"),
                            Stage.sortBy(Sort.asc(id)),
                          )
                        )
                        .all
        yield joined.map(user => user.getString("id").getValue -> user.getArray("big").size)

      program.timeout(30.seconds).asserting(_ shouldBe List("u1" -> 1, "u2" -> 1))
    }
  }
