package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.bson.FieldNaming
import mongo4s.operations.Filter

import mongo4s.bson.BsonInstances.given

object FilterSpec:
  final case class Address(city: String, zipCode: String)
  final case class User(firstName: String, age: Int, address: Address)

final class FilterSpec extends AnyWordSpec with Matchers:
  import FilterSpec.User

  "Field.of" should {
    "extract a single segment" in {
      Field.of[User, String](_.firstName).path.segments shouldBe List("firstName")
    }

    "extract a nested path" in {
      Field.of[User, String](_.address.city).path.segments shouldBe List("address", "city")
    }
  }

  "Filter" should {
    "render field names through the naming policy" in {
      val filter = Field.of[User, String](_.firstName).equalTo("bob")
      filter.toBson(FieldNaming.snakeCase).toJson shouldBe """{"first_name": "bob"}"""
    }

    "render nested paths segment by segment" in {
      val filter = Field.of[User, String](_.address.zipCode).equalTo("12345")
      filter.toBson(FieldNaming.snakeCase).toJson shouldBe """{"address.zip_code": "12345"}"""
    }

    "treat an empty in-clause as matching nothing" in {
      val filter = Field.of[User, Int](_.age).in(Nil)
      filter shouldBe Filter.MatchNone[User]()
      filter.toBson(FieldNaming.identity).toJson shouldBe """{"$nor": [{}]}"""
    }

    "combine conditions" in {
      val filter = Field.of[User, Int](_.age).gt(18) && Field.of[User, String](_.firstName).equalTo("bob")
      filter.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$and": [{"age": {"$gt": 18}}, {"first_name": "bob"}]}"""
    }
  }

  "PrimaryKey" should {
    "build an equality filter for a compound key" in {
      val pk: PrimaryKey[User, (String, Int)] = PrimaryKey.make(
        user => (user.firstName, user.age),
        key => "first_name" -> key._1,
        key => "age"        -> key._2,
      )

      pk.eqFilter(("bob", 30)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$and": [{"first_name": "bob"}, {"age": 30}]}"""
    }

    "build an $in filter for a single-field key" in {
      val pk = PrimaryKey.single[User, String]("first_name")(_.firstName)

      pk.inFilter(List("bob", "alice")).toBson(FieldNaming.identity).toJson shouldBe
        """{"first_name": {"$in": ["bob", "alice"]}}"""
    }

    "match nothing for an empty key list" in {
      val pk = PrimaryKey.single[User, String]("first_name")(_.firstName)
      pk.inFilter(Nil) shouldBe Filter.MatchNone[User]()
    }
  }
