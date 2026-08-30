package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.bson.FieldNaming
import mongo4s.operations.Filter

import mongo4s.bson.BsonInstances.given

object FilterSpec:
  final case class Address(city: String, zipCode: String)
  final case class User(firstName: String, age: Int, address: Address, tags: List[String], totalsByCurrency: Map[String, Int])

final class FilterSpec extends AnyWordSpec with Matchers:
  import FilterSpec.{Address, User}

  "Field.of" should {
    "extract a single segment" in {
      Field.of[User, String](_.firstName).path shouldBe FieldPath.derived(List("firstName"))
    }

    "extract a nested path" in {
      Field.of[User, String](_.address.city).path shouldBe FieldPath.derived(List("address", "city"))
    }

    "reject a selector that is not a chain of case-class fields" in {
      "Field.of[User, Int](_.firstName.length)" shouldNot typeCheck
      "Field.of[User, String](_.firstName.toLowerCase)" shouldNot typeCheck
    }
  }

  "FieldPath./" should {
    "leave the appended segment alone while still renaming the derived prefix" in {
      val path = Field.of[User, Address](_.address).path / "zipCode"
      path.render(FieldNaming.snakeCase) shouldBe "address.zipCode"
    }
  }

  "stored segments under a field" should {
    "keep a map key verbatim while renaming the field that holds it" in {
      val filter = Field.of[User, Map[String, Int]](_.totalsByCurrency).at("EUR").equalTo(10)

      filter.toBson(FieldNaming.snakeCase).toJson shouldBe """{"totals_by_currency.EUR": 10}"""
    }

    "reach any other stored segment through /" in {
      val firstTag: Field[User, String] = Field.of[User, List[String]](_.tags) / "0"

      firstTag.equalTo("vip").toBson(FieldNaming.snakeCase).toJson shouldBe """{"tags.0": "vip"}"""
    }

    "still be reachable from the collection root" in {
      Field.stored[User, String]("_id").equalTo("x").toBson(FieldNaming.snakeCase).toJson shouldBe """{"_id": "x"}"""
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
      val pk: PrimaryKey[User, (String, Int)] =
        PrimaryKey.compound[User, (String, Int), String, Int](user => (user.firstName, user.age))(
          "first_name",
          _._1,
        )("age", _._2)

      pk.eqFilter(("bob", 30)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$and": [{"first_name": "bob"}, {"age": 30}]}"""
    }

    "expose its stored field names, so the enforcing index can be built from it" in {
      val pk: PrimaryKey[User, (String, Int)] =
        PrimaryKey.compound[User, (String, Int), String, Int](user => (user.firstName, user.age))(
          "first_name",
          _._1,
        )("age", _._2)

      pk.fieldNames shouldBe List("first_name", "age")
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
