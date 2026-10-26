package mongo4s.internal

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.Field
import mongo4s.operations.*
import mongo4s.bson.FieldNaming

object DriverOptionsSpec:
  final case class Order(itemCount: Int)

final class DriverOptionsSpec extends AnyWordSpec, Matchers:
  import DriverOptionsSpec.Order

  private val itemCount = Field.of[Order, Int](_.itemCount)
  private val variables = BsonDocument("limit", BsonInt32(3))

  "insert options" should {
    "keep inserts ordered unless asked otherwise" in {
      DriverOptions.insertMany(InsertManyOptions.default).isOrdered shouldBe true
      DriverOptions.insertMany(InsertManyOptions.unordered).isOrdered shouldBe false
    }

    "carry a comment and bypassDocumentValidation, one and many" in {
      val one  = DriverOptions.insertOne(InsertOneOptions.default.withComment("one").withBypassDocumentValidation(true))
      val many = DriverOptions.insertMany(InsertManyOptions.default.withComment("many").withBypassDocumentValidation(true))

      (one.getComment, one.getBypassDocumentValidation) shouldBe (BsonString("one"), true)
      (many.getComment, many.getBypassDocumentValidation) shouldBe (BsonString("many"), true)
    }
  }

  "bulk write options" should {
    "carry ordering, a comment, bypassDocumentValidation and let variables" in {
      val options = DriverOptions.bulkWrite(BulkWriteOptions.unordered.withComment("bulk").withBypassDocumentValidation(true).withLet(variables))

      (options.isOrdered, options.getComment, options.getBypassDocumentValidation, options.getLet) shouldBe (false, BsonString("bulk"), true, variables)
    }
  }

  "update and replace options" should {
    "render a sort through the collection's naming, and carry let variables" in {
      val update  = DriverOptions.update(UpdateOptions.default.withSort(Sort.asc(itemCount)).withLet(variables), FieldNaming.snakeCase)
      val replace = DriverOptions.replace(ReplaceOptions.default.withSort(Sort.desc(itemCount)).withLet(variables), FieldNaming.snakeCase)

      (update.getSort, update.getLet) shouldBe (BsonDocument("item_count", BsonInt32(1)), variables)
      (replace.getSort, replace.getLet) shouldBe (BsonDocument("item_count", BsonInt32(-1)), variables)
    }

    "refuse a sort on updateMany, which would otherwise be dropped" in {
      an[IllegalArgumentException] should be thrownBy
        DriverOptions.updateMany(UpdateOptions.default.withSort(Sort.asc(itemCount)), FieldNaming.identity)
    }
  }

  "the remaining write options" should {
    "carry let variables on deletes and every findOneAnd*, and a comment on counts" in {
      DriverOptions.delete(DeleteOptions.default.withLet(variables)).getLet shouldBe variables
      DriverOptions.findOneAndUpdate(FindOneAndUpdateOptions.default[Order].withLet(variables), FieldNaming.identity).getLet shouldBe variables
      DriverOptions.findOneAndReplace(FindOneAndReplaceOptions.default[Order].withLet(variables), FieldNaming.identity).getLet shouldBe variables
      DriverOptions.findOneAndDelete(FindOneAndDeleteOptions.default[Order].withLet(variables), FieldNaming.identity).getLet shouldBe variables
      DriverOptions.count(CountOptions.default.withComment("counted")).getComment shouldBe BsonString("counted")
    }
  }
