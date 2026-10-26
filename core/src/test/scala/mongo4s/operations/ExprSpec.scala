package mongo4s.operations

import java.time.Instant

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.Field
import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object ExprSpec:
  final case class Item(skuCode: String, unitPrice: Double, qty: Int)
  final case class Order(orderId: String, unitPrice: Double, qty: Int, discount: Option[Double], items: List[Item], createdAt: Instant, note: String)

final class ExprSpec extends AnyWordSpec, Matchers:
  import ExprSpec.{Item, Order}

  private val unitPrice = Field.of[Order, Double](_.unitPrice)
  private val qty       = Field.of[Order, Int](_.qty)
  private val discount  = Field.of[Order, Option[Double]](_.discount)
  private val items     = Field.of[Order, List[Item]](_.items)
  private val createdAt = Field.of[Order, Instant](_.createdAt)
  private val remark    = Field.of[Order, String](_.note)
  private val itemQty   = Field.of[Item, Int](_.qty)
  private val itemPrice = Field.of[Item, Double](_.unitPrice)

  private def snake(expression: Expr[Order, ?]): String = BsonDocument("e", expression.toBson(FieldNaming.snakeCase)).toJson
  private def snake(stage: Stage[Order]): String        = stage.toBson(FieldNaming.snakeCase).toJson

  "field references and literals" should {
    "spell a field through the naming, and wrap a literal only where the server would read it as something else" in {
      snake(unitPrice.expr) shouldBe """{"e": "$unit_price"}"""
      snake(Expr.lit(5)) shouldBe """{"e": 5}"""
      snake(Expr.lit("$notAField")) shouldBe """{"e": {"$literal": "$notAField"}}"""
    }

    "read an Option field as the value it holds" in {
      typeChecks("val e: Expr[ExprSpec.Order, Double] = discount.expr") shouldBe true
    }
  }

  "arithmetic" should {
    "combine numbers of different widths into the wider one" in {
      val total: Expr[Order, Double] = unitPrice.expr * qty.expr
      snake(total) shouldBe """{"e": {"$multiply": ["$unit_price", "$qty"]}}"""

      val summed: Expr[Order, Int] = qty.expr + Expr.lit(1)
      snake(summed) shouldBe """{"e": {"$add": ["$qty", 1]}}"""
    }

    "divide into a Double, and fall back on a value where a field is null" in {
      snake(qty.expr / Expr.lit(4)) shouldBe """{"e": {"$divide": ["$qty", 4]}}"""
      snake(discount.expr.orElse(Expr.lit(0.0))) shouldBe """{"e": {"$ifNull": ["$discount", 0.0]}}"""
    }

    "refuse to compile on text, or between a number and text" in {
      typeChecks("remark.expr * qty.expr") shouldBe false
      typeChecks("qty.expr + remark.expr") shouldBe false
    }
  }

  "comparisons and logic" should {
    "render each comparison, and combine them with $and, $or and $not" in {
      snake((unitPrice.expr > Expr.lit(10.0)) && !qty.expr.equalTo(Expr.lit(0))) shouldBe
        """{"e": {"$and": [{"$gt": ["$unit_price", 10.0]}, {"$not": [{"$eq": ["$qty", 0]}]}]}}"""
    }

    "compare only values of one type" in {
      typeChecks("qty.expr < remark.expr") shouldBe false
    }

    "choose between values with $cond and $switch" in {
      snake(Expr.cond(qty.expr > Expr.lit(10), Expr.lit("bulk"), Expr.lit("single"))) shouldBe
        """{"e": {"$cond": {"if": {"$gt": ["$qty", 10]}, "then": "bulk", "else": "single"}}}"""

      snake(Expr.switch((qty.expr > Expr.lit(100)) -> Expr.lit("huge"), (qty.expr > Expr.lit(10)) -> Expr.lit("big"))(Expr.lit("small"))) shouldBe
        """{"e": {"$switch": {"branches": [{"case": {"$gt": ["$qty", 100]}, "then": "huge"}, {"case": {"$gt": ["$qty", 10]}, "then": "big"}], "default": "small"}}}"""
    }
  }

  "strings" should {
    "concatenate, change case, measure and match text" in {
      snake(remark.expr.toUpper ++ Expr.lit("!")) shouldBe """{"e": {"$concat": [{"$toUpper": "$note"}, "!"]}}"""
      snake(remark.expr.length) shouldBe """{"e": {"$strLenCP": "$note"}}"""
      snake(remark.expr.matches("^a", "i")) shouldBe """{"e": {"$regexMatch": {"input": "$note", "regex": "^a", "options": "i"}}}"""
    }
  }

  "arrays" should {
    "filter and map their elements through a typed variable, its fields spelt through the naming" in {
      snake(items.expr.filter(item => item.get(itemQty) > Expr.lit(2))) shouldBe
        """{"e": {"$filter": {"input": "$items", "as": "this", "cond": {"$gt": ["$$this.qty", 2]}}}}"""

      snake(items.expr.map(item => item.get(itemPrice) * item.get(itemQty))) shouldBe
        """{"e": {"$map": {"input": "$items", "as": "this", "in": {"$multiply": ["$$this.unit_price", "$$this.qty"]}}}}"""
    }

    "take a size, an element and membership" in {
      snake(items.expr.size) shouldBe """{"e": {"$size": "$items"}}"""
      snake(items.expr.at(0).get(itemQty)) shouldBe """{"e": {"$getField": {"field": "qty", "input": {"$arrayElemAt": ["$items", 0]}}}}"""
    }

    "compile only over an array" in {
      typeChecks("qty.expr.size") shouldBe false
    }
  }

  "dates" should {
    "take parts, truncate, format, add and diff" in {
      snake(createdAt.expr.year) shouldBe """{"e": {"$year": "$created_at"}}"""
      snake(createdAt.expr.truncated(DateUnit.Month)) shouldBe """{"e": {"$dateTrunc": {"date": "$created_at", "unit": "month", "binSize": 1}}}"""
      snake(createdAt.expr.formatAs("%Y-%m")) shouldBe """{"e": {"$dateToString": {"date": "$created_at", "format": "%Y-%m"}}}"""
      snake(createdAt.expr.until(Expr.now, DateUnit.Day)) shouldBe """{"e": {"$dateDiff": {"startDate": "$created_at", "endDate": "$$NOW", "unit": "day"}}}"""
    }

    "compile only on a date" in {
      typeChecks("qty.expr.year") shouldBe false
    }
  }

  "the stages that take expressions" should {
    "compute fields with $addFields, through the naming for a model field and as given for a free name" in {
      snake(Stage.addFields(unitPrice := unitPrice.expr * Expr.lit(1.2), "line_total" := unitPrice.expr * qty.expr)) shouldBe
        """{"$addFields": {"unit_price": {"$multiply": ["$unit_price", 1.2]}, "line_total": {"$multiply": ["$unit_price", "$qty"]}}}"""
    }

    "group by an expression, or by a compound key, accumulating an expression" in {
      snake(Stage.groupByExpr(createdAt.expr.year)("revenue" -> Accumulator.sum(unitPrice.expr * qty.expr))) shouldBe
        """{"$group": {"_id": {"$year": "$created_at"}, "revenue": {"$sum": {"$multiply": ["$unit_price", "$qty"]}}}}"""

      snake(Stage.groupByKeys("year" := createdAt.expr.year, "month" := createdAt.expr.month)("orders" -> Accumulator.count[Order])) shouldBe
        """{"$group": {"_id": {"year": {"$year": "$created_at"}, "month": {"$month": "$created_at"}}, "orders": {"$sum": 1}}}"""
    }

    "replace the root with an expression, and redact with one" in {
      snake(Stage.replaceWith(Expr.mergeObjects(Expr.root[Order], Expr.document("total" := unitPrice.expr * qty.expr)))) shouldBe
        """{"$replaceWith": {"$mergeObjects": ["$$ROOT", {"total": {"$multiply": ["$unit_price", "$qty"]}}]}}"""

      snake(Stage.redact(Expr.cond(qty.expr > Expr.lit(0), Expr.descend, Expr.prune))) shouldBe
        """{"$redact": {"$cond": {"if": {"$gt": ["$qty", 0]}, "then": "$$DESCEND", "else": "$$PRUNE"}}}"""
    }

    "project a computed field, including the search score" in {
      Projection.empty[Order].include(qty).compute("score" := Expr.searchScore).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"qty": 1, "score": {"$meta": "searchScore"}}"""
    }

    "filter with a typed $expr, and keep the raw one" in {
      Filter.expr(unitPrice.expr > qty.expr.toDouble).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$expr": {"$gt": ["$unit_price", {"$toDouble": "$qty"}]}}"""

      Filter.expr[Order](BsonDocument("$gt", BsonString("x"))).toBson(FieldNaming.identity).toJson shouldBe """{"$expr": {"$gt": "x"}}"""
    }

    "refuse an empty $addFields and an empty compound key" in {
      an[IllegalArgumentException] should be thrownBy Stage.addFields[Order]()
      an[IllegalArgumentException] should be thrownBy Stage.groupByKeys[Order]()("n" -> Accumulator.count[Order])
    }
  }

  "variables" should {
    "refuse names the server would refuse" in {
      an[IllegalArgumentException] should be thrownBy Expr.variable[Int]("Min")
      BsonDocument("v", Expr.variable[Int]("min").toBson(FieldNaming.identity)).toJson shouldBe """{"v": "$$min"}"""
      BsonDocument("v", BsonInt32(1)).toJson shouldBe """{"v": 1}"""
    }
  }
