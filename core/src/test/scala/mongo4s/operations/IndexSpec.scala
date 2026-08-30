package mongo4s.operations

import scala.concurrent.duration.*

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

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

    "render a text index as a marker rather than a direction" in {
      Index.empty[User].text(bioField).keysToBson(FieldNaming.identity).toJson shouldBe """{"bio": "text"}"""
    }
  }

  "forKeyFields" should {
    // A PrimaryKey knows its stored names, and the index that enforces it must use them verbatim —
    // the collection's naming has already been applied when the key was declared.
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

    // expireAfterSeconds is an integer on the server, so anything under a second would truncate to
    // 0 — which is not "no TTL", it is "expire the moment the date field is reached".
    "reject a TTL that would truncate to zero seconds" in {
      val thrown = the[IllegalArgumentException] thrownBy Index.ascending(ageField).expiringAfter(500.millis)
      thrown.getMessage should include("at least one second")
    }
  }
