package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object PrimaryKeyOfSpec:
  final case class Link(provider: String, providerId: Long, itemType: String, note: String)

final class PrimaryKeyOfSpec extends AnyWordSpec, Matchers:
  import PrimaryKeyOfSpec.Link

  private val link = Link("dst", 42L, "competitor", "n")

  private def json(filter: operations.Filter[Link]): String = filter.toBson(FieldNaming.snakeCase).toJson

  "PrimaryKey.of" should {

    "key on one field, spelled through the collection's naming when it renders" in {
      val pk = PrimaryKey.of[Link, Long](_.providerId)

      pk.key(link) shouldBe 42L
      json(pk.eqFilter(42L)) shouldBe """{"provider_id": 42}"""
      json(pk.inFilter(List(1L, 2L))) shouldBe """{"provider_id": {"$in": [1, 2]}}"""
    }

    "key on two or three fields as a plain tuple" in {
      val two   = PrimaryKey.of[Link, String, Long](_.provider, _.providerId)
      val three = PrimaryKey.of[Link, String, Long, String](_.provider, _.providerId, _.itemType)

      two.key(link) shouldBe ("dst", 42L)
      three.key(link) shouldBe ("dst", 42L, "competitor")
      json(three.eqFilter(("dst", 42L, "competitor"))) shouldBe """{"$and": [{"provider": "dst"}, {"provider_id": 42}, {"item_type": "competitor"}]}"""
      json(two.inFilter(List(("a", 1L), ("b", 2L)))) shouldBe
        """{"$or": [{"$and": [{"provider": "a"}, {"provider_id": 1}]}, {"$and": [{"provider": "b"}, {"provider_id": 2}]}]}"""
    }

    "order and page by the same renamed fields" in {
      val two = PrimaryKey.of[Link, String, Long](_.provider, _.providerId)

      two.keyOrder(descending = true).toBson(FieldNaming.snakeCase).toJson shouldBe """{"provider": -1, "provider_id": -1}"""
      json(two.afterFilter(("a", 1L))) shouldBe """{"$or": [{"provider": {"$gt": "a"}}, {"$and": [{"provider": "a"}, {"provider_id": {"$gt": 1}}]}]}"""
    }

    "be the key of the entity it is applied to" in {
      import PrimaryKey.primaryKeyFilter

      given PrimaryKey[Link, (String, Long)] = PrimaryKey.of(_.provider, _.providerId)

      json(link.primaryKeyFilter) shouldBe """{"$and": [{"provider": "dst"}, {"provider_id": 42}]}"""
    }

    "refuse a selector that is not a field, and a field named twice" in {
      "PrimaryKey.of[PrimaryKeyOfSpec.Link, Int](_.note.length)" shouldNot typeCheck
      an[IllegalArgumentException] should be thrownBy PrimaryKey.of[Link, String, String](_.provider, _.provider)
    }
  }

  "a key over stored names" should {

    "render exactly as it did, through keyPaths" in {
      val pk = PrimaryKey.single[Link, Long]("providerId")(_.providerId)

      pk.keyPaths shouldBe List(FieldPath.literal("providerId"))
      json(pk.eqFilter(1L)) shouldBe """{"providerId": 1}"""
    }
  }
