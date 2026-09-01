package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

import scala.concurrent.duration.given
import mongo4s.bson.BsonInstances.given

object IndexSpec:
  final case class User(firstName: String, age: Int, bio: String)

final class IndexSpec extends AnyWordSpec, Matchers:
  import IndexSpec.User

  private val nameField = Field.of[User, String](_.firstName)
  private val ageField  = Field.of[User, Int](_.age)
  private val bioField  = Field.of[User, String](_.bio)

  "index keys" should {
    "render direction per field, in declaration order" in {
      val index = Index.ascending(nameField).descending(ageField)

      index.keysToBson(FieldNaming.identity).toJson shouldBe """{"firstName": 1, "age": -1}"""
    }

    "route derived names through the naming policy" in {
      Index.ascending(nameField).keysToBson(FieldNaming.snakeCase).toJson shouldBe """{"first_name": 1}"""
    }

    "let the last mention of a field win its priority, not just its direction" in {
      val index = Index.ascending(nameField).ascending(ageField).descending(nameField)

      index.keysToBson(FieldNaming.identity).toJson shouldBe """{"age": 1, "firstName": -1}"""
    }

    "render hashed and geospatial indexes as their own markers" in {
      Index.hashed(nameField).keysToBson(FieldNaming.identity).toJson shouldBe """{"firstName": "hashed"}"""
      Index.geo2dsphere(nameField).keysToBson(FieldNaming.identity).toJson shouldBe """{"firstName": "2dsphere"}"""
      Index.empty[User].geo2d(nameField).keysToBson(FieldNaming.identity).toJson shouldBe """{"firstName": "2d"}"""
    }

    "index a wildcard path, which needs no direction of its own" in {
      val index = Index.ascending(Field.stored[User, Any]("$**"))

      index.keysToBson(FieldNaming.snakeCase).toJson shouldBe """{"$**": 1}"""
    }

    "render a text index as a marker rather than a direction" in {
      Index.empty[User].text(bioField).keysToBson(FieldNaming.identity).toJson shouldBe """{"bio": "text"}"""
    }
  }

  "forKeyFields" should {
    "build a unique index over stored names, untouched by naming" in {
      val index = Index.forKeyFields[User](List("user_id", "seq"))

      index.unique shouldBe true
      index.keysToBson(FieldNaming.snakeCase).toJson shouldBe """{"user_id": 1, "seq": 1}"""
    }
  }

  "options" should {
    "carry unique, sparse, name and partial filter" in {
      val index = Index.ascending(nameField).withUnique.withSparse.named("by_name").where(ageField.gte(18))

      index.unique shouldBe true
      index.sparse shouldBe true
      index.name shouldBe Some("by_name")
      index.partialFilter.map(_.toBson(FieldNaming.identity).toJson) shouldBe Some("""{"age": {"$gte": 18}}""")
    }

    "accept a TTL of whole seconds" in {
      Index.ascending(ageField).expiringAfter(30.days).expireAfter shouldBe Some(30.days)
    }

    "reject a TTL that would truncate to zero seconds" in {
      val thrown = the[IllegalArgumentException] thrownBy Index.ascending(ageField).expiringAfter(500.millis)
      thrown.getMessage should include("at least one second")
    }
  }
