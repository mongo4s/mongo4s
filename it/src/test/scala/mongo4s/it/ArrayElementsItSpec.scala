package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonTimestamp, BsonType}

import mongo4s.cats.CatsStream
import mongo4s.bson.BsonTypeName
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.operations.{Sort, Stage, Update, UpdateOptions}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ArrayElementsItSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Line(unitPrice: Int, skuCode: String) derives WireCodec
  final case class Basket(basketId: String, lineItems: List[Line], recentScores: List[Int]) derives WireCodec

final class ArrayElementsItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ArrayElementsItSpec.{Basket, Line}

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val basketId     = Field.of[Basket, String](_.basketId)
  private val lineItems    = Field.of[Basket, List[Line]](_.lineItems)
  private val recentScores = Field.of[Basket, List[Int]](_.recentScores)
  private val unitPrice    = Field.of[Line, Int](_.unitPrice)
  private val skuCode      = Field.of[Line, String](_.skuCode)

  private val basket = Basket("1", List(Line(1, "a"), Line(5, "b"), Line(2, "c")), List(3, 7, 9, 12))

  private def seeded(name: String) =
    for
      client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database <- client.getDatabase("array_elements_it")
      baskets  <- database.getDirectCollection[Basket](name)
      _        <- baskets.insertOne(basket)
    yield (database, baskets)

  "the typed positional operators" should {
    "update every element through $[], spelling the nested field through the codec's naming" in {
      val program =
        for
          (_, baskets) <- seeded("each")
          _            <- baskets.updateOne(basketId.equalTo("1"), Update.set(lineItems.each / unitPrice, 0))
          found        <- baskets.find(basketId.equalTo("1")).first
        yield found.map(_.lineItems.map(_.unitPrice))

      program.asserting(_ shouldBe Some(List(0, 0, 0)))
    }

    "update the element the query matched through $" in {
      val program =
        for
          (_, baskets) <- seeded("matched")
          _            <- baskets.updateOne(lineItems.elemMatch(skuCode.equalTo("b")), Update.set(lineItems.matched / unitPrice, 50))
          found        <- baskets.find(basketId.equalTo("1")).first
        yield found.map(_.lineItems.map(_.unitPrice))

      program.asserting(_ shouldBe Some(List(1, 50, 2)))
    }

    "update the elements an array filter built on the same element selects, through $[identifier]" in {
      val cheap = Field.element[Line]("cheap")
      val high  = Field.element[Int]("high")

      val program =
        for
          (_, baskets) <- seeded("filtered")
          _            <- baskets.updateOne(
                            basketId.equalTo("1"),
                            Update.inc(lineItems.where(cheap) / unitPrice, 10),
                            UpdateOptions.default.withArrayFilters(Seq((cheap / unitPrice).lt(3))),
                          )
          _            <- baskets.updateOne(
                            basketId.equalTo("1"),
                            Update.set(recentScores.where(high), 0),
                            UpdateOptions.default.withArrayFilters(Seq(high.gte(9))),
                          )
          found        <- baskets.find(basketId.equalTo("1")).first
        yield found.map(found => (found.lineItems.map(_.unitPrice), found.recentScores))

      program.asserting(_ shouldBe Some((List(11, 5, 12), List(3, 7, 0, 0))))
    }
  }

  "$pull with a condition" should {
    "remove the document elements and the scalar elements its condition matches" in {
      val program =
        for
          (_, baskets) <- seeded("pull_where")
          _            <- baskets.updateOne(basketId.equalTo("1"), lineItems.pullWhere(unitPrice.lt(3)))
          _            <- baskets.updateOne(basketId.equalTo("1"), recentScores.pullWhere(Field.self[Int].gte(7) && Field.self[Int].lt(10)))
          found        <- baskets.find(basketId.equalTo("1")).first
        yield found.map(found => (found.lineItems.map(_.skuCode), found.recentScores))

      program.asserting(_ shouldBe Some((List("b"), List(3, 12))))
    }
  }

  "a condition on a scalar element itself" should {
    "match a range inside $elemMatch, which no single comparison could" in {
      val program =
        for
          (_, baskets) <- seeded("elem_match_range")
          inside       <- baskets.count(recentScores.elemMatch(Field.self[Int].gt(7) && Field.self[Int].lt(9)))
          between      <- baskets.count(recentScores.elemMatch(Field.self[Int].gt(7) && Field.self[Int].lt(10)))
        yield (inside, between)

      program.asserting(_ shouldBe (0L, 1L))
    }
  }

  "$type over several types and $currentDate as a timestamp" should {
    "match any of the types, and store a BSON timestamp" in {
      val synced = Field.stored[Basket, BsonTimestamp]("synced")

      val program =
        for
          (database, baskets) <- seeded("types")
          _                   <- baskets.updateOne(basketId.equalTo("1"), Update.currentTimestamp(synced))
          raw                 <- database.getCollection[BsonDocument]("types").flatMap(_.find().first)
          anyType             <- baskets.count(basketId.hasAnyType(BsonTypeName.Int, BsonTypeName.String))
          wrongTypes          <- baskets.count(basketId.hasAnyType(BsonTypeName.Int, BsonTypeName.Long))
        yield (raw.map(_.get("synced").getBsonType), anyType, wrongTypes)

      program.asserting(_ shouldBe (Some(BsonType.TIMESTAMP), 1L, 0L))
    }
  }

  "$unwind and $unionWith" should {
    "number each unwound element, and union another collection narrowed by its pipeline" in {
      val program =
        for
          (database, baskets) <- seeded("unwound")
          archive             <- database.getDirectCollection[Basket]("archive")
          _                   <- archive.insertMany(List(Basket("2", Nil, Nil), Basket("3", Nil, Nil)))
          positions           <-
            database
              .getCollection[BsonDocument]("unwound")
              .flatMap(
                _.aggregate[BsonDocument](Seq(Stage.unwind(Field.stored[BsonDocument, List[BsonDocument]]("line_items"), includeArrayIndex = Some("position")))).all
              )
          unioned             <- baskets
                                   .aggregateDirect[Basket](
                                     Seq(
                                       Stage.unionWith[Basket, Basket]("archive", List(Stage.matching(basketId.equalTo("3")))),
                                       Stage.sortBy(Sort.asc(basketId)),
                                     )
                                   )
                                   .all
        yield (positions.map(_.getInt64("position").getValue), unioned.map(_.basketId))

      program.timeout(30.seconds).asserting(_ shouldBe (List(0L, 1L, 2L), List("1", "3")))
    }
  }
