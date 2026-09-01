package mongo4s.results

import java.util.LinkedHashMap

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonString, BsonValue}

final class InsertManyResultSpec extends AnyWordSpec, Matchers:

  "InsertManyResult.fromDriver" should {
    "order ids by command position, not by the driver map's iteration order" in {
      val insertedIds = LinkedHashMap[Integer, BsonValue]()
      insertedIds.put(Int.box(2), BsonString("c"))
      insertedIds.put(Int.box(0), BsonString("a"))
      insertedIds.put(Int.box(1), BsonString("b"))

      InsertManyResult.fromDriver(insertedIds).insertedIds shouldBe List(BsonString("a"), BsonString("b"), BsonString("c"))
    }
  }
