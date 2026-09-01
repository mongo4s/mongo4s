package mongo4s.results

import java.util.LinkedHashMap

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonString, BsonValue}
import com.mongodb.client.result.InsertManyResult as DriverInsertManyResult

final class InsertManyResultSpec extends AnyWordSpec, Matchers:

  "InsertManyResult.fromDriver" should {
    "order ids by command position, not by the driver map's iteration order" in {
      val insertedIds = LinkedHashMap[Integer, BsonValue]()
      insertedIds.put(Int.box(2), BsonString("c"))
      insertedIds.put(Int.box(0), BsonString("a"))
      insertedIds.put(Int.box(1), BsonString("b"))

      val result = DriverInsertManyResult.acknowledged(insertedIds)

      InsertManyResult.fromDriver(result).insertedIds shouldBe List(BsonString("a"), BsonString("b"), BsonString("c"))
    }
  }
