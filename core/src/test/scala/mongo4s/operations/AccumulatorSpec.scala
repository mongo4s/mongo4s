package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

object AccumulatorSpec:
  final case class Order(userId: String, itemCount: Int, total: Double, placedBy: String, details: Map[String, String])

final class AccumulatorSpec extends AnyWordSpec, Matchers:
  import AccumulatorSpec.Order

  private val itemCount = Field.of[Order, Int](_.itemCount)
  private val total     = Field.of[Order, Double](_.total)
  private val userId    = Field.of[Order, String](_.userId)
  private val details   = Field.of[Order, Map[String, String]](_.details)

  private def json(accumulator: Accumulator[Order]): String = accumulator.toBson(FieldNaming.snakeCase).toJson

  "the n-element accumulators" should {

    "render their input through the naming policy, and n beside it" in {
      json(Accumulator.firstN(itemCount, 3)) shouldBe """{"$firstN": {"input": "$item_count", "n": 3}}"""
      json(Accumulator.lastN(itemCount, 3)) shouldBe """{"$lastN": {"input": "$item_count", "n": 3}}"""
      json(Accumulator.maxN(itemCount, 2)) shouldBe """{"$maxN": {"input": "$item_count", "n": 2}}"""
      json(Accumulator.minN(itemCount, 2)) shouldBe """{"$minN": {"input": "$item_count", "n": 2}}"""
    }

    "refuse an n below one, which the server refuses" in {
      an[IllegalArgumentException] should be thrownBy Accumulator.firstN(itemCount, 0)
      an[IllegalArgumentException] should be thrownBy Accumulator.maxN(itemCount, -1)
      an[IllegalArgumentException] should be thrownBy Accumulator.topN(Sort.desc(total), userId, 0)
    }
  }

  "the ranking accumulators" should {

    "render the sort they rank by and the field they output" in {
      json(Accumulator.top(Sort.desc(total), userId)) shouldBe
        """{"$top": {"sortBy": {"total": -1}, "output": "$user_id"}}"""
      json(Accumulator.bottom(Sort.desc(total), userId)) shouldBe
        """{"$bottom": {"sortBy": {"total": -1}, "output": "$user_id"}}"""
      json(Accumulator.topN(Sort.desc(total).asc(userId), itemCount, 2)) shouldBe
        """{"$topN": {"sortBy": {"total": -1, "user_id": 1}, "output": "$item_count", "n": 2}}"""
      json(Accumulator.bottomN(Sort.asc(total), itemCount, 2)) shouldBe
        """{"$bottomN": {"sortBy": {"total": 1}, "output": "$item_count", "n": 2}}"""
    }

    "refuse an empty sort, since there would be nothing to rank by" in {
      the[IllegalArgumentException] thrownBy Accumulator.top(Sort.empty[Order], userId) should have message
        "requirement failed: $top ranks by its sortBy, so it needs at least one field to sort on"
    }
  }

  "the statistical accumulators" should {

    "render median and percentile with the only method the server offers" in {
      json(Accumulator.median(total)) shouldBe """{"$median": {"input": "$total", "method": "approximate"}}"""
      json(Accumulator.percentile(total, 0.5, 0.9)) shouldBe
        """{"$percentile": {"input": "$total", "method": "approximate", "p": [0.5, 0.9]}}"""
    }

    "refuse a percentile outside 0 to 1" in {
      an[IllegalArgumentException] should be thrownBy Accumulator.percentile(total, 1.5)
      an[IllegalArgumentException] should be thrownBy Accumulator.percentile(total, 0.5, -0.1)
    }

    "render both standard deviations" in {
      json(Accumulator.stdDevPop(itemCount)) shouldBe """{"$stdDevPop": "$item_count"}"""
      json(Accumulator.stdDevSamp(itemCount)) shouldBe """{"$stdDevSamp": "$item_count"}"""
    }

    "compile only on a numeric field" in {
      "Accumulator.median(Field.of[AccumulatorSpec.Order, String](_.userId))" shouldNot typeCheck
      "Accumulator.percentile(Field.of[AccumulatorSpec.Order, String](_.userId), 0.5)" shouldNot typeCheck
      "Accumulator.stdDevPop(Field.of[AccumulatorSpec.Order, String](_.userId))" shouldNot typeCheck
    }
  }

  "$mergeObjects" should {
    "render the field it merges" in {
      json(Accumulator.mergeObjects(details)) shouldBe """{"$mergeObjects": "$details"}"""
    }
  }
