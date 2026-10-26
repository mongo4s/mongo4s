package mongo4s.operations

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonTimestamp

import mongo4s.Field
import mongo4s.bson.{BsonTypeName, FieldNaming}

import mongo4s.bson.BsonInstances.given

object ArrayElementsSpec:
  final case class Line(unitPrice: Int, skuCode: String)
  final case class Basket(lineItems: List[Line], recentScores: List[Int], lastSync: BsonTimestamp)

final class ArrayElementsSpec extends AnyWordSpec, Matchers:
  import ArrayElementsSpec.{Basket, Line}

  private val lineItems    = Field.of[Basket, List[Line]](_.lineItems)
  private val recentScores = Field.of[Basket, List[Int]](_.recentScores)
  private val unitPrice    = Field.of[Line, Int](_.unitPrice)
  private val skuCode      = Field.of[Line, String](_.skuCode)

  private def snake(update: Update[Basket]): String = update.toBson(FieldNaming.snakeCase).toJson
  private def snake(filter: Filter[?]): String      = filter.toBson(FieldNaming.snakeCase).toJson

  "the positional operators" should {
    "render $[] for every element, spelling the fields around it through the naming" in {
      snake(Update.set(lineItems.each / unitPrice, 0)) shouldBe """{"$set": {"line_items.$[].unit_price": 0}}"""
    }

    "render $ for the element the query matched" in {
      snake(Update.set(lineItems.matched / skuCode, "x")) shouldBe """{"$set": {"line_items.$.sku_code": "x"}}"""
    }

    "render $[identifier] from the same element its array filters are built on" in {
      val cheap = Field.element[Line]("cheap")

      snake(Update.inc(lineItems.where(cheap) / unitPrice, 1)) shouldBe """{"$inc": {"line_items.$[cheap].unit_price": 1}}"""
      snake((cheap / unitPrice).lt(3)) shouldBe """{"cheap.unit_price": {"$lt": 3}}"""
    }

    "filter a scalar array element on the element itself" in {
      val high = Field.element[Int]("high")

      snake(Update.set(recentScores.where(high), 100)) shouldBe """{"$set": {"recent_scores.$[high]": 100}}"""
      snake(high.gte(90)) shouldBe """{"high": {"$gte": 90}}"""
    }

    "refuse an identifier the server would refuse" in {
      an[IllegalArgumentException] should be thrownBy Field.element[Line]("Cheap")
      an[IllegalArgumentException] should be thrownBy Field.element[Line]("low-qty")
    }

    "compile only on an array field, and keep the element's type" in {
      typeChecks("Field.of[ArrayElementsSpec.Basket, BsonTimestamp](_.lastSync).each") shouldBe false
      typeChecks("Update.set(lineItems.each / unitPrice, \"text\")") shouldBe false
    }
  }

  "a condition on the element itself" should {
    "render as the bare operator, so it reads as the element's own condition" in {
      snake(Field.self[Int].gte(6)) shouldBe """{"$gte": 6}"""
      snake(Field.self[Int].equalTo(6)) shouldBe """{"$eq": 6}"""
      snake(Field.self[String].regex("^a")) shouldBe """{"$regex": {"$regularExpression": {"pattern": "^a", "options": ""}}}"""
    }

    "merge its conjuncts into one document, which is how $elemMatch takes a range" in {
      val range = Field.self[Int].gte(80) && Field.self[Int].lt(85)

      snake(recentScores.elemMatch(range)) shouldBe """{"recent_scores": {"$elemMatch": {"$gte": 80, "$lt": 85}}}"""
    }
  }

  "$pull with a condition" should {
    "remove the document elements a filter on their fields matches" in {
      snake(lineItems.pullWhere(unitPrice.lt(3))) shouldBe """{"$pull": {"line_items": {"unit_price": {"$lt": 3}}}}"""
    }

    "remove the scalar elements a condition on themselves matches" in {
      snake(recentScores.pullWhere(Field.self[Int].gte(6) && Field.self[Int].lt(9))) shouldBe
        """{"$pull": {"recent_scores": {"$gte": 6, "$lt": 9}}}"""
    }

    "compile only with a condition over the array's own element type" in {
      typeChecks("lineItems.pullWhere(Field.self[Int].gte(6))") shouldBe false
    }
  }

  "$type" should {
    "take one type as a string and several as an array" in {
      snake(unitPrice.hasType(BsonTypeName.Int)) shouldBe """{"unit_price": {"$type": "int"}}"""
      snake(unitPrice.hasAnyType(BsonTypeName.Int, BsonTypeName.Long)) shouldBe """{"unit_price": {"$type": ["int", "long"]}}"""
    }
  }

  "$currentDate" should {
    "set a timestamp when the field holds one" in {
      snake(Update.currentTimestamp(Field.of[Basket, BsonTimestamp](_.lastSync))) shouldBe
        """{"$currentDate": {"last_sync": {"$type": "timestamp"}}}"""
    }
  }

  "$unwind and $unionWith" should {
    "name the field that receives each element's index" in {
      Stage.unwind(lineItems, includeArrayIndex = Some("position")).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$unwind": {"path": "$line_items", "preserveNullAndEmptyArrays": false, "includeArrayIndex": "position"}}"""
    }

    "take a pipeline over the other collection, and stay a bare name without one" in {
      Stage.unionWith[Basket]("archive").toBson(FieldNaming.identity).toJson shouldBe """{"$unionWith": "archive"}"""

      Stage.unionWith[Basket, Basket]("archive", List(Stage.limit(5))).toBson(FieldNaming.identity).toJson shouldBe
        """{"$unionWith": {"coll": "archive", "pipeline": [{"$limit": 5}]}}"""
    }
  }
