package mongo4s.it

import java.time.Instant

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32}

import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient, MongoCollection}
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.operations.{Accumulator, DateUnit, Expr, Filter, Projection, Sort, Stage, Update}
import mongo4s.operations.:=

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ExprItSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Item(skuCode: String, unitPrice: Double, qty: Int) derives WireCodec

  final case class Order(
      orderId: String,
      unitPrice: Double,
      qty: Int,
      discount: Option[Double],
      items: List[Item],
      createdAt: Instant,
      note: String,
  ) derives WireCodec

final class ExprItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ExprItSpec.{Item, Order}

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val orderId   = Field.of[Order, String](_.orderId)
  private val unitPrice = Field.of[Order, Double](_.unitPrice)
  private val qty       = Field.of[Order, Int](_.qty)
  private val discount  = Field.of[Order, Option[Double]](_.discount)
  private val items     = Field.of[Order, List[Item]](_.items)
  private val createdAt = Field.of[Order, Instant](_.createdAt)
  private val remark    = Field.of[Order, String](_.note)
  private val itemQty   = Field.of[Item, Int](_.qty)
  private val itemPrice = Field.of[Item, Double](_.unitPrice)
  private val itemSku   = Field.of[Item, String](_.skuCode)

  private val orders = List(
    Order("a", 2.5, 4, Some(0.5), List(Item("x", 1.0, 3), Item("y", 2.0, 1)), Instant.parse("2026-03-14T10:15:00Z"), "first order"),
    Order("b", 10.0, 1, None, List(Item("z", 10.0, 1)), Instant.parse("2026-03-20T08:00:00Z"), "Second"),
    Order("c", 1.0, 20, None, Nil, Instant.parse("2025-12-31T23:59:59Z"), "bulk"),
  )

  private def seeded(name: String): IO[MongoCollection[IO, S, Order]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("expr_it")
      collection <- database.getDirectCollection[Order](name)
      _          <- collection.insertMany(orders)
    yield collection

  private def computed(name: String)(fields: mongo4s.operations.Computed[Order]*): IO[Map[String, BsonDocument]] =
    seeded(name).flatMap { collection =>
      collection
        .aggregate[BsonDocument](Seq(Stage.addFields(fields*), Stage.sortBy(Sort.asc(orderId))))
        .all
        .map(_.map(document => document.getString("order_id").getValue -> document).toMap)
    }

  "arithmetic, conditionals and conversions" should {
    "compute on the server what the expression says, across number widths and through null" in {
      val program = computed("arithmetic")(
        "total" := unitPrice.expr * qty.expr,
        "net"   := unitPrice.expr * qty.expr - discount.expr.orElse(Expr.lit(0.0)),
        "half"  := qty.expr / Expr.lit(2),
        "size"  := Expr.switch((qty.expr >= Expr.lit(10)) -> Expr.lit("bulk"), (qty.expr > Expr.lit(1)) -> Expr.lit("some"))(Expr.lit("one")),
        "label" := Expr.cond(discount.expr.orElse(Expr.lit(0.0)) > Expr.lit(0.0), Expr.lit("discounted"), Expr.lit("full")),
        "text"  := qty.expr.asString,
      )

      program.asserting { result =>
        result.view.mapValues(_.getDouble("total").getValue).toMap shouldBe Map("a" -> 10.0, "b" -> 10.0, "c" -> 20.0)
        result("a").getDouble("net").getValue shouldBe 9.5
        result("c").getDouble("half").getValue shouldBe 10.0
        result.view.mapValues(_.getString("size").getValue).toMap shouldBe Map("a" -> "some", "b" -> "one", "c" -> "bulk")
        result.view.mapValues(_.getString("label").getValue).toMap shouldBe Map("a" -> "discounted", "b" -> "full", "c" -> "full")
        result("c").getString("text").getValue shouldBe "20"
      }
    }
  }

  "strings" should {
    "concatenate, change case, measure and match on the server" in {
      val program = computed("strings")(
        "shout"   := remark.expr.toUpper ++ Expr.lit("!"),
        "length"  := remark.expr.length,
        "capital" := remark.expr.matches("^[A-Z]"),
        "words"   := remark.expr.split(" "),
      )

      program.asserting { result =>
        result("a").getString("shout").getValue shouldBe "FIRST ORDER!"
        result("a").getInt32("length").getValue shouldBe 11
        result.view.mapValues(_.getBoolean("capital").getValue).toMap shouldBe Map("a" -> false, "b" -> true, "c" -> false)
        result("a").getArray("words").size shouldBe 2
      }
    }
  }

  "arrays" should {
    "filter, map, size and index through a typed element variable spelt through the naming" in {
      val program = computed("arrays")(
        "many"     := items.expr.filter(item => item.get(itemQty) > Expr.lit(1)),
        "lines"    := items.expr.map(item => item.get(itemPrice) * item.get(itemQty)),
        "count"    := items.expr.size,
        "firstSku" := items.expr.at(0).get(itemSku),
      )

      program.asserting { result =>
        result("a").getArray("many").size shouldBe 1
        result("a").getArray("lines").getValues.toArray.toList.map(_.asInstanceOf[org.bson.BsonDouble].getValue) shouldBe List(3.0, 2.0)
        result.view.mapValues(_.getInt32("count").getValue).toMap shouldBe Map("a" -> 2, "b" -> 1, "c" -> 0)
        result("b").getString("firstSku").getValue shouldBe "z"
      }
    }
  }

  "dates" should {
    "take parts, truncate, format, add and diff on the server" in {
      val program = computed("dates")(
        "year"  := createdAt.expr.year,
        "month" := createdAt.expr.truncated(DateUnit.Month),
        "label" := createdAt.expr.formatAs("%Y-%m-%d"),
        "later" := createdAt.expr.plus(DateUnit.Day, Expr.lit(1L)),
        "days"  := createdAt.expr.until(Expr.lit(Instant.parse("2026-04-01T00:00:00Z")), DateUnit.Day),
      )

      program.asserting { result =>
        result.view.mapValues(_.getInt32("year").getValue).toMap shouldBe Map("a" -> 2026, "b" -> 2026, "c" -> 2025)
        result("a").getDateTime("month").getValue shouldBe Instant.parse("2026-03-01T00:00:00Z").toEpochMilli
        result("a").getString("label").getValue shouldBe "2026-03-14"
        result("a").getDateTime("later").getValue shouldBe Instant.parse("2026-03-15T10:15:00Z").toEpochMilli
        result("b").getInt64("days").getValue shouldBe 12L
      }
    }
  }

  "the stages that take expressions" should {
    "group by an expression and by a compound key, summing an expression" in {
      val program =
        for
          collection <- seeded("grouped")
          byYear     <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.groupByExpr(createdAt.expr.year)("revenue" -> Accumulator.sum(unitPrice.expr * qty.expr)),
                              Stage.sortBy(Sort.asc(Field.stored[Order, Int]("_id")))
                            )
                          )
                          .all
          byMonth    <-
            collection
              .aggregate[BsonDocument](Seq(Stage.groupByKeys("year" := createdAt.expr.year, "month" := createdAt.expr.month)("orders" -> Accumulator.count[Order])))
              .all
        yield (byYear.map(group => group.getInt32("_id").getValue -> group.getDouble("revenue").getValue), byMonth.size)

      program.asserting(_ shouldBe (List(2025 -> 20.0, 2026 -> 20.0), 2))
    }

    "replace the root with a merged document, and redact with a condition" in {
      val program =
        for
          collection <- seeded("replaced")
          replaced   <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.matching(orderId.equalTo("a")),
                              Stage.replaceWith(Expr.mergeObjects(Expr.document("id" := orderId.expr), Expr.document("total" := unitPrice.expr * qty.expr))),
                            )
                          )
                          .first
          redacted   <- collection.aggregate[BsonDocument](Seq(Stage.redact(Expr.cond(qty.expr > Expr.lit(1), Expr.keep, Expr.prune)))).all
        yield (replaced, redacted.size)

      program.asserting(_ shouldBe (Some(BsonDocument("id", org.bson.BsonString("a")).append("total", org.bson.BsonDouble(10.0))), 2))
    }

    "compute a field in a find projection, and filter with a typed $expr" in {
      val program =
        for
          collection <- seeded("projected")
          found      <- collection
                          .find(Filter.expr(unitPrice.expr * qty.expr >= Expr.lit(10.0)))
                          .sort(Sort.asc(orderId))
                          .selecting(
                            Projection.empty[Order].include(orderId).compute("total" := unitPrice.expr * qty.expr).withoutId,
                            _ => summon[mongo4s.bson.BsonDocumentDecoder[BsonDocument]]
                          )
                          .all
        yield found.map(document => document.getString("order_id").getValue -> document.getDouble("total").getValue)

      program.asserting(_ shouldBe List("a" -> 10.0, "b" -> 10.0, "c" -> 20.0))
    }

    "compute an update from the document's own values in a pipeline update, and read a let variable" in {
      val program =
        for
          collection <- seeded("updated")
          _          <- collection.updateMany(Filter.all, Update.pipeline(Stage.addFields(qty := qty.expr * Expr.lit(2))))
          doubled    <- collection.find(orderId.equalTo("c")).first
          over       <- collection.find(Filter.expr(qty.expr > Expr.variable[Int]("limit"))).let(BsonDocument("limit", BsonInt32(5))).all
        yield (doubled.map(_.qty), over.map(_.orderId).sorted)

      program.asserting(_ shouldBe (Some(40), List("a", "c")))
    }
  }
