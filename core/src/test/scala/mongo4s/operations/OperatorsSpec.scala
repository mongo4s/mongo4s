package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object OperatorsSpec:
  final case class Item(sku: String, quantity: Int)
  final case class Order(id: String, total: Int, items: List[Item], labels: Set[String])

final class OperatorsSpec extends AnyWordSpec, Matchers:
  import OperatorsSpec.{Item, Order}

  private val itemsField  = Field.of[Order, List[Item]](_.items)
  private val labelsField = Field.of[Order, Set[String]](_.labels)
  private val totalField  = Field.of[Order, Int](_.total)

  private val skuField      = Field.of[Item, String](_.sku)
  private val quantityField = Field.of[Item, Int](_.quantity)

  private def json(filter: Filter[Order]): String = filter.toBson(FieldNaming.identity).toJson

  "$elemMatch" should {
    "require a single element to satisfy every condition" in {
      val filter = itemsField.elemMatch(skuField.equalTo("abc") && quantityField.gt(2))

      json(filter) shouldBe """{"items": {"$elemMatch": {"$and": [{"sku": "abc"}, {"quantity": {"$gt": 2}}]}}}"""
    }
  }

  "array predicates" should {
    "match an element by equality" in {
      json(labelsField.contains("urgent")) shouldBe """{"labels": "urgent"}"""
    }

    "match arrays containing every value" in {
      json(labelsField.containsAll(List("a", "b"))) shouldBe """{"labels": {"$all": ["a", "b"]}}"""
    }

    "treat containsAll of nothing as no constraint" in {
      json(labelsField.containsAll(Nil)) shouldBe "{}"
    }

    "match by array length" in {
      json(itemsField.hasSize(3)) shouldBe """{"items": {"$size": 3}}"""
    }
  }

  "other predicates" should {
    "render $type and $mod" in {
      json(totalField.hasType("int")) shouldBe """{"total": {"$type": "int"}}"""
      json(totalField.mod(4, 1)) shouldBe """{"total": {"$mod": [4, 1]}}"""
    }

    "render text search" in {
      json(Filter.text[Order]("scala")) shouldBe """{"$text": {"$search": "scala"}}"""
    }
  }

  "$group" should {
    "render a grouped aggregation with accumulators" in {
      val stage = Stage.groupBy(totalField)("count" -> Accumulator.count[Order], "skus" -> Accumulator.push(itemsField))

      stage.toBson(FieldNaming.identity).toJson shouldBe
        """{"$group": {"_id": "$total", "count": {"$sum": 1}, "skus": {"$push": "$items"}}}"""
    }

    "render a single total group" in {
      val stage = Stage.groupAll[Order]("total" -> Accumulator.sum(totalField))

      stage.toBson(FieldNaming.identity).toJson shouldBe """{"$group": {"_id": null, "total": {"$sum": "$total"}}}"""
    }

    "route field references through the naming policy" in {
      val stage = Stage.groupAll[Order]("total" -> Accumulator.sum(totalField))

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$group": {"_id": null, "total": {"$sum": "$total"}}}"""
    }
  }

  "$lookup" should {
    "leave the foreign field unrenamed" in {
      val stage = Stage.lookup[Order, String]("customers", Field.of[Order, String](_.id), "orderId", "customer")

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$lookup": {"from": "customers", "localField": "id", "foreignField": "orderId", "as": "customer"}}"""
    }
  }

  "Projection" should {
    "reject exclusion after inclusion rather than silently dropping the included fields" in {
      an[IllegalArgumentException] should be thrownBy Projection.empty[Order].include(totalField).exclude(itemsField)
    }

    "reject inclusion after a non-_id exclusion for the same reason" in {
      an[IllegalArgumentException] should be thrownBy Projection.empty[Order].exclude(itemsField).include(totalField)
    }

    "allow inclusion on top of an _id-only exclusion, keeping _id out" in {
      val projection = Projection.excludeId[Order].include(totalField)

      projection.toBson(FieldNaming.identity).toJson shouldBe """{"total": 1, "_id": 0}"""
    }

    "keep _id excludable from an inclusion projection" in {
      val projection = Projection.empty[Order].include(totalField).withoutId

      projection.toBson(FieldNaming.identity).toJson shouldBe """{"total": 1, "_id": 0}"""
    }
  }
