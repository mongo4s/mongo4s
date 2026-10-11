package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.Field
import mongo4s.bson.FieldNaming
import mongo4s.operations.geometry.Geometry

import mongo4s.bson.BsonInstances.given

object StageSpec:
  final case class Order(userId: String, itemCount: Int, tags: List[String])
  final case class User(id: String, age: Int, managerId: String)

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

    "render a $lookup driven by a sub-pipeline over the foreign collection" in {
      val stage = Stage.lookupWith[Order, StageSpec.User](
        from = "users",
        pipeline = List(Stage.matching(Field.of[StageSpec.User, Int](_.age).gte(18))),
        as = "adults",
        let = Some(BsonDocument("owner", BsonString("$user_id"))),
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$lookup": {"from": "users", "let": {"owner": "$user_id"}, "pipeline": [{"$match": {"age": {"$gte": 18}}}], "as": "adults"}}"""
    }

    "render a $graphLookup, with its options only when they are set" in {
      val bare = Stage.graphLookup[Order, StageSpec.User, String](
        from = "users",
        startWith = BsonString("$user_id"),
        connectFrom = Field.of[StageSpec.User, String](_.managerId),
        connectTo = Field.of[StageSpec.User, String](_.id),
        as = "chain",
      )

      bare.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$graphLookup": {"from": "users", "startWith": "$user_id", "connectFromField": "manager_id", "connectToField": "id", "as": "chain"}}"""

      val tuned = Stage.graphLookup[Order, StageSpec.User, String](
        from = "users",
        startWith = BsonString("$user_id"),
        connectFrom = Field.of[StageSpec.User, String](_.managerId),
        connectTo = Field.of[StageSpec.User, String](_.id),
        as = "chain",
        options = GraphLookupOptions.default[StageSpec.User].withMaxDepth(3).withDepthField("depth"),
      )

      tuned.toBson(FieldNaming.snakeCase).toJson should include(""""maxDepth": 3, "depthField": "depth"""")
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

    "render $bucket with typed boundaries and its output accumulators" in {
      val stage = Stage.bucketBy(Field.of[Order, Int](_.itemCount), Seq(0, 10, 100), default = Some(BsonString("more")))(
        "count" -> Accumulator.count[Order]
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$bucket": {"groupBy": "$item_count", "boundaries": [0, 10, 100], "default": "more", "output": {"count": {"$sum": 1}}}}"""
    }

    "render $unset as a name for one field and as an array for several, through the naming policy" in {
      Stage.unset(Field.of[Order, Int](_.itemCount)).toBson(FieldNaming.snakeCase).toJson shouldBe """{"$unset": "item_count"}"""

      Stage.unset(Field.of[Order, Int](_.itemCount)).and(Field.of[Order, String](_.userId)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$unset": ["item_count", "user_id"]}"""
    }

    "render $sortByCount as the field's value, through the naming policy" in {
      Stage.sortByCount(Field.of[Order, String](_.userId)).toBson(FieldNaming.snakeCase).toJson shouldBe """{"$sortByCount": "$user_id"}"""
    }

    "render $bucketAuto with the number of buckets and its output accumulators" in {
      val stage = Stage.bucketAuto(Field.of[Order, String](_.userId), 4)("count" -> Accumulator.count[Order])

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$bucketAuto": {"groupBy": "$user_id", "buckets": 4, "output": {"count": {"$sum": 1}}}}"""
    }

    "render a rounded $bucketAuto with its granularity, over a numeric field only" in {
      Stage.bucketAutoRounded(Field.of[Order, Int](_.itemCount), 3, BucketGranularity.OneTwoFive)().toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$bucketAuto": {"groupBy": "$item_count", "buckets": 3, "granularity": "1-2-5"}}"""

      "Stage.bucketAutoRounded(Field.of[StageSpec.Order, String](_.userId), 3, BucketGranularity.R5)()" shouldNot typeCheck
    }

    "refuse a $bucketAuto with no buckets to fill" in {
      an[IllegalArgumentException] should be thrownBy Stage.bucketAuto(Field.of[Order, Int](_.itemCount), 0)()
    }

    "render $geoNear with the key it searches, and its options only when they are set" in {
      val location = Field.stored[Order, Geometry.Point]("location")
      val centre   = Geometry.Point(13.4, 52.5): Geometry.Point

      Stage.geoNear(location, centre, "distance").toBson(FieldNaming.identity).toJson shouldBe
        """{"$geoNear": {"near": {"type": "Point", "coordinates": [13.4, 52.5]}, "distanceField": "distance", "key": "location"}}"""

      val options = GeoNearOptions
        .default[Order]
        .where(Field.of[Order, Int](_.itemCount).gte(1))
        .withMaxDistance(500)
        .withDistanceMultiplier(0.001)
        .withIncludeLocs("at")

      Stage.geoNear(location, centre, "distance", options).toBson(FieldNaming.snakeCase).toJson should include(
        """"query": {"item_count": {"$gte": 1}}, "maxDistance": 500.0, "distanceMultiplier": 0.001, "includeLocs": "at""""
      )
    }

    "refuse a negative $geoNear distance before the server is asked" in {
      an[IllegalArgumentException] should be thrownBy GeoNearOptions.default[Order].withMaxDistance(-1)
      an[IllegalArgumentException] should be thrownBy GeoNearOptions.default[Order].withMinDistance(Double.NaN)
    }

    "render window functions beside accumulators in one $setWindowFields, each in the shape the server reads" in {
      val count = Field.of[Order, Int](_.itemCount)

      val stage = Stage.setWindowFields(Sort.asc(count))(
        "total" -> WindowOutput(Accumulator.sum(count)),
        "place" -> WindowFunction.rank[Order],
        "dense" -> WindowFunction.denseRank[Order],
        "row"   -> WindowFunction.documentNumber[Order],
        "prev"  -> WindowFunction.shift(count, by = -1, default = Some(0)),
        "kept"  -> WindowFunction.locf(count),
        "fill"  -> WindowFunction.linearFill(count),
        "slope" -> WindowFunction.derivative(count, Window.range(WindowBound.At(-1), WindowBound.Current), Some(DateUnit.Hour)),
        "area"  -> WindowFunction.integral(count, Window.documents(WindowBound.Unbounded, WindowBound.Current)),
        "ema"   -> WindowFunction.expMovingAvg(count, 3),
        "emaA"  -> WindowFunction.expMovingAvgAlpha(count, 0.25),
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$setWindowFields": {"sortBy": {"item_count": 1}, "output": {""" +
        """"total": {"$sum": "$item_count"}, "place": {"$rank": {}}, "dense": {"$denseRank": {}}, "row": {"$documentNumber": {}}, """ +
        """"prev": {"$shift": {"output": "$item_count", "by": -1, "default": 0}}, "kept": {"$locf": "$item_count"}, """ +
        """"fill": {"$linearFill": "$item_count"}, """ +
        """"slope": {"$derivative": {"input": "$item_count", "unit": "hour"}, "window": {"range": [-1, "current"]}}, """ +
        """"area": {"$integral": {"input": "$item_count"}, "window": {"documents": ["unbounded", "current"]}}, """ +
        """"ema": {"$expMovingAvg": {"input": "$item_count", "N": 3}}, "emaA": {"$expMovingAvg": {"input": "$item_count", "alpha": 0.25}}}}}"""
    }

    "refuse an exponential moving average the server cannot weigh" in {
      an[IllegalArgumentException] should be thrownBy WindowFunction.expMovingAvg(Field.of[Order, Int](_.itemCount), 0)
      an[IllegalArgumentException] should be thrownBy WindowFunction.expMovingAvgAlpha(Field.of[Order, Int](_.itemCount), 1.0)
      "WindowFunction.linearFill(Field.of[StageSpec.Order, String](_.userId))" shouldNot typeCheck
    }

    "refuse a $bucket that cannot make a single bucket" in {
      an[IllegalArgumentException] should be thrownBy
        Stage.bucketBy(Field.of[Order, Int](_.itemCount), Seq(0))()
    }

    "render $densify over a numeric field" in {
      val stage = Stage.densify(
        Field.of[Order, Int](_.itemCount),
        DensifyRange.by(5).within(DensifyBounds.Between(BsonInt32(0), BsonInt32(50))),
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$densify": {"field": "item_count", "range": {"step": 5, "bounds": [0, 50]}}}"""
    }

    "render $densify over a date field, partitioned" in {
      val stage = Stage.densify(
        Field.of[Order, Int](_.itemCount),
        DensifyRange.every(1, DateUnit.Hour).within(DensifyBounds.Partition),
        partitionBy = Seq(Field.of[Order, String](_.userId).path),
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$densify": {"field": "item_count", "partitionByFields": ["user_id"], "range": {"step": 1, "bounds": "partition", "unit": "hour"}}}"""
    }

    "render $setWindowFields with a partition, a sort and a window" in {
      val stage = Stage.setWindowFields(
        Sort.asc(Field.of[Order, Int](_.itemCount)),
        partitionBy = Some(Field.of[Order, String](_.userId).path),
      )(
        "runningTotal" -> WindowOutput(Accumulator.sum(Field.of[Order, Int](_.itemCount)))
          .over(Window.documents(WindowBound.Unbounded, WindowBound.Current))
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$setWindowFields": {"partitionBy": "$user_id", "sortBy": {"item_count": 1}, """ +
        """"output": {"runningTotal": {"$sum": "$item_count", "window": {"documents": ["unbounded", "current"]}}}}}"""
    }

    "let an accumulator with no window cover the whole partition" in {
      val stage = Stage.setWindowFields(Sort.asc(Field.of[Order, Int](_.itemCount)))(
        "total" -> WindowOutput(Accumulator.sum(Field.of[Order, Int](_.itemCount)))
      )

      stage.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$setWindowFields": {"sortBy": {"item_count": 1}, "output": {"total": {"$sum": "$item_count"}}}}"""
    }

    "refuse a $setWindowFields that computes nothing" in {
      an[IllegalArgumentException] should be thrownBy
        Stage.setWindowFields(Sort.asc(Field.of[Order, Int](_.itemCount)))()
    }

    "pass a Raw stage through untouched" in {
      val document = BsonDocument("$sample", BsonDocument("size", org.bson.BsonInt32(10)))
      Stage.raw[Order](document).toBson(FieldNaming.identity) shouldBe document
    }
  }
