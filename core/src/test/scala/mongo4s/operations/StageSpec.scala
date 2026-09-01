package mongo4s.operations

import org.bson.BsonDocument
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object StageSpec:
  final case class Order(userId: String, itemCount: Int, tags: List[String])

final class StageSpec extends AnyWordSpec, Matchers:
  import StageSpec.Order

  "Stage" should {
    "render $match from a Filter, through the naming policy" in {
      val stage = Stage.matching(Field.of[Order, String](_.userId).equalTo("u1"))
      stage.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$match": {"user_id": "u1"}}"""
    }

    "render $project from a Projection" in {
      val stage = Stage.project(Projection.empty[Order].include(Field.of[Order, Int](_.itemCount)))
      stage.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$project": {"item_count": 1}}"""
    }

    "render $sort from a Sort" in {
      val stage = Stage.sortBy(Sort.desc(Field.of[Order, Int](_.itemCount)))
      stage.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$sort": {"item_count": -1}}"""
    }

    "render $limit" in {
      Stage.limit[Order](5).toBson(FieldNaming.identity).toJson shouldBe """{"$limit": 5}"""
    }

    "render $skip" in {
      Stage.skip[Order](3).toBson(FieldNaming.identity).toJson shouldBe """{"$skip": 3}"""
    }

    "render $count" in {
      Stage.count[Order]("total").toBson(FieldNaming.identity).toJson shouldBe """{"$count": "total"}"""
    }

    "render $unwind with preserveNullAndEmptyArrays defaulting to false, through the naming policy" in {
      val stage = Stage.unwind(Field.of[Order, List[String]](_.tags))
      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": false}}"""
    }

    "render $unwind with preserveNullAndEmptyArrays overridden" in {
      val stage = Stage.unwind(Field.of[Order, List[String]](_.tags), preserveNullAndEmptyArrays = true)
      stage.toBson(FieldNaming.identity).toJson shouldBe
        """{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}"""
    }

    "render $lookup, rendering only the local field through the naming policy" in {
      val stage = Stage.lookup("users", Field.of[Order, String](_.userId), "user_id", as = "user")
      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$lookup": {"from": "users", "localField": "user_id", "foreignField": "user_id", "as": "user"}}"""
    }

    "render $out and $merge as a bare name while nothing else is set" in {
      Stage.out[Order]("archive").toBson(FieldNaming.identity).toJson shouldBe """{"$out": "archive"}"""
      Stage.merge[Order]("archive").toBson(FieldNaming.identity).toJson shouldBe """{"$merge": "archive"}"""
    }

    "name the database when one is given" in {
      Stage.out[Order]("archive", OutOptions.default.inDatabase("cold")).toBson(FieldNaming.identity).toJson shouldBe
        """{"$out": {"db": "cold", "coll": "archive"}}"""
    }

    "render a single $merge on-field as a string and several as an array" in {
      val one  = MergeOptions.default.onFields(List("user_id"))
      val many = MergeOptions.default.onFields(List("user_id", "seq"))

      Stage.merge[Order]("archive", one).toBson(FieldNaming.identity).toJson shouldBe
        """{"$merge": {"into": "archive", "on": "user_id"}}"""
      Stage.merge[Order]("archive", many).toBson(FieldNaming.identity).toJson shouldBe
        """{"$merge": {"into": "archive", "on": ["user_id", "seq"]}}"""
    }

    "carry the whenMatched and whenNotMatched policies" in {
      val options = MergeOptions.default
        .inDatabase("cold")
        .whenMatched(MergeOptions.WhenMatched.KeepExisting)
        .whenNotMatched(MergeOptions.WhenNotMatched.Discard)

      Stage.merge[Order]("archive", options).toBson(FieldNaming.identity).toJson shouldBe
        """{"$merge": {"into": {"db": "cold", "coll": "archive"}, "whenMatched": "keepExisting", "whenNotMatched": "discard"}}"""
    }

    "pass a Raw stage through untouched" in {
      val document = BsonDocument("$sample", BsonDocument("size", org.bson.BsonInt32(10)))
      Stage.raw[Order](document).toBson(FieldNaming.identity) shouldBe document
    }
  }
