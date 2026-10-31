package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.bson.FieldNaming
import mongo4s.operations.Filter

import mongo4s.bson.BsonInstances.given

object FilterSpec:
  opaque type AccountId = Long

  object AccountId:
    def apply(value: Long): AccountId = value

    given mongo4s.bson.direct.ScalarWireCodec[AccountId] = mongo4s.bson.direct.ScalarWireCodec[Long].imap(AccountId.apply)(identity)

  final case class Account(id: AccountId, owners: List[AccountId])

  final case class Address(city: String, zipCode: String)
  final case class User(
      firstName: String,
      age: Int,
      address: Address,
      tags: List[String],
      totalsByCurrency: Map[String, Int],
      homeAddress: Option[Address],
      pastAddresses: List[Address],
      formerAddresses: Option[Vector[Address]],
      nickNames: Option[List[String]],
  )

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

  "a typed path through an Option or into the elements of a collection" should {
    val zipCode = Field.of[Address, String](_.zipCode)

    "continue into the fields of an optional document, renaming every segment" in {
      val zip = Field.of[User, Option[Address]](_.homeAddress) / zipCode

      zip.equalTo("12345").toBson(FieldNaming.snakeCase).toJson shouldBe """{"home_address.zip_code": "12345"}"""
    }

    "continue into the fields of a collection's elements, the way MongoDB traverses an array" in {
      val past   = Field.of[User, List[Address]](_.pastAddresses) / zipCode
      val former = Field.of[User, Option[Vector[Address]]](_.formerAddresses) / zipCode

      past.equalTo("12345").toBson(FieldNaming.snakeCase).toJson shouldBe """{"past_addresses.zip_code": "12345"}"""
      former.in(List("1", "2")).toBson(FieldNaming.snakeCase).toJson shouldBe """{"former_addresses.zip_code": {"$in": ["1", "2"]}}"""
    }

    "serve wherever a field does — a sort, a projection, an index" in {
      val past = Field.of[User, List[Address]](_.pastAddresses) / zipCode

      operations.Sort.asc(past).toBson(FieldNaming.snakeCase).toJson shouldBe """{"past_addresses.zip_code": 1}"""
      operations.Projection.empty[User].include(past).toBson(FieldNaming.snakeCase).toJson shouldBe """{"past_addresses.zip_code": 1}"""
      operations.Index.ascending(past).keysToBson(FieldNaming.snakeCase).toJson shouldBe """{"past_addresses.zip_code": 1}"""
    }

    "still compose an exact type the way it did" in {
      (Field.of[User, Address](_.address) / zipCode).path.render(FieldNaming.snakeCase) shouldBe "address.zip_code"
    }

    "not compose through a field that holds no such document" in {
      "Field.of[FilterSpec.User, Int](_.age) / Field.of[FilterSpec.Address, String](_.zipCode)" shouldNot typeCheck
      "Field.stored[FilterSpec.User, Option[String]](\"nick\") / Field.of[FilterSpec.Address, String](_.zipCode)" shouldNot typeCheck
      "Field.of[FilterSpec.User, List[String]](_.tags) / Field.of[FilterSpec.Address, String](_.zipCode)" shouldNot typeCheck
    }
  }

  "array operators on an optional collection" should {
    val nickNames = Field.of[User, Option[List[String]]](_.nickNames)

    "apply the way they do to the collection itself" in {
      nickNames.contains("bob").toBson(FieldNaming.snakeCase).toJson shouldBe """{"nick_names": "bob"}"""
      nickNames.push("bob").toBson(FieldNaming.snakeCase).toJson shouldBe """{"$push": {"nick_names": "bob"}}"""
      nickNames.hasSize(2).toBson(FieldNaming.snakeCase).toJson shouldBe """{"nick_names": {"$size": 2}}"""
    }

    "still refuse an element of another type" in {
      "Field.of[FilterSpec.User, Option[List[String]]](_.nickNames).contains(1)" shouldNot typeCheck
    }
  }

  "a field typed by an id that has only a ScalarWireCodec" should {
    import FilterSpec.{Account, AccountId}

    "filter, update and key on it with no BsonEncoder written for it" in {
      val id     = Field.of[Account, AccountId](_.id)
      val owners = Field.of[Account, List[AccountId]](_.owners)

      id.in(List(AccountId(1L), AccountId(2L))).toBson(FieldNaming.identity).toJson shouldBe """{"id": {"$in": [1, 2]}}"""
      owners.push(AccountId(3L)).toBson(FieldNaming.identity).toJson shouldBe """{"$push": {"owners": 3}}"""
      PrimaryKey.single[Account, AccountId]("id")(_.id).eqFilter(AccountId(4L)).toBson(FieldNaming.identity).toJson shouldBe """{"id": 4}"""
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
      filter.toBson(FieldNaming.identity).toJson shouldBe """{"_id": {"$in": []}}"""
    }

    "render matching nothing on _id, which every collection indexes, rather than as a $nor the planner scans for" in {
      Filter.none[User].toBson(FieldNaming.snakeCase).toJson shouldBe """{"_id": {"$in": []}}"""
    }

    "render the empty disjunction and the negations of all and none as what they mean" in {
      Filter.Or[User](Nil).toBson(FieldNaming.identity).toJson shouldBe """{"_id": {"$in": []}}"""
      Filter.And[User](Nil).toBson(FieldNaming.identity).toJson shouldBe "{}"
      (!Filter.all[User]).toBson(FieldNaming.identity).toJson shouldBe """{"_id": {"$in": []}}"""
      (!Filter.none[User]).toBson(FieldNaming.identity).toJson shouldBe "{}"
    }

    "combine conditions" in {
      val filter = Field.of[User, Int](_.age).gt(18) && Field.of[User, String](_.firstName).equalTo("bob")
      filter.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$and": [{"age": {"$gt": 18}}, {"first_name": "bob"}]}"""
    }

    "chain into one flat $and or $or rather than nesting one inside the next" in {
      val age   = Field.of[User, Int](_.age)
      val chain = age.gt(18) && age.lt(65) && age.notEqualTo(30)
      val alts  = age.equalTo(1) || age.equalTo(2) || age.equalTo(3)

      chain.toBson(FieldNaming.identity).toJson shouldBe """{"$and": [{"age": {"$gt": 18}}, {"age": {"$lt": 65}}, {"age": {"$ne": 30}}]}"""
      alts.toBson(FieldNaming.identity).toJson shouldBe """{"$or": [{"age": 1}, {"age": 2}, {"age": 3}]}"""
    }

    "keep an $or nested inside an $and, since flattening it would change what matches" in {
      val age = Field.of[User, Int](_.age)

      (age.gt(18) && (age.equalTo(1) || age.equalTo(2))).toBson(FieldNaming.identity).toJson shouldBe
        """{"$and": [{"age": {"$gt": 18}}, {"$or": [{"age": 1}, {"age": 2}]}]}"""
    }
  }

  "PrimaryKey" should {
    "build an equality filter for a compound key" in {
      val pk: PrimaryKey[User, (firstName: String, age: Int)] =
        PrimaryKey.compound(user => (firstName = user.firstName, age = user.age), FieldNaming.snakeCase)

      pk.eqFilter((firstName = "bob", age = 30)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"$and": [{"first_name": "bob"}, {"age": 30}]}"""
    }

    "reject a compound key whose labels are written in another order" in {
      val pk: PrimaryKey[User, (firstName: String, age: Int)] =
        PrimaryKey.compound(user => (firstName = user.firstName, age = user.age), FieldNaming.snakeCase)

      pk.fieldNames shouldBe List("first_name", "age")
      """pk.eqFilter((age = 30, firstName = "bob"))""" shouldNot typeCheck
    }

    "expose its stored field names, so the enforcing index can be built from it" in {
      val pk: PrimaryKey[User, (firstName: String, age: Int)] =
        PrimaryKey.compound(user => (firstName = user.firstName, age = user.age), FieldNaming.snakeCase)

      pk.fieldNames shouldBe List("first_name", "age")
    }

    "step past a single-field key with a plain comparison" in {
      val pk = PrimaryKey.single[User, String]("first_name")(_.firstName)

      pk.afterFilter("bob").toBson(FieldNaming.identity).toJson shouldBe """{"first_name": {"$gt": "bob"}}"""
    }

    "step past a compound key in lexicographic order" in {
      val pk: PrimaryKey[User, (firstName: String, age: Int)] =
        PrimaryKey.compound(user => (firstName = user.firstName, age = user.age), FieldNaming.snakeCase)

      pk.afterFilter((firstName = "bob", age = 30)).toBson(FieldNaming.identity).toJson shouldBe
        """{"$or": [{"first_name": {"$gt": "bob"}}, {"$and": [{"first_name": "bob"}, {"age": {"$gt": 30}}]}]}"""
    }

    "order by the key's own fields, in the order it declares them" in {
      val pk: PrimaryKey[User, (firstName: String, age: Int)] =
        PrimaryKey.compound(user => (firstName = user.firstName, age = user.age), FieldNaming.snakeCase)

      pk.keyOrder.toBson(FieldNaming.identity).toJson shouldBe """{"first_name": 1, "age": 1}"""
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
