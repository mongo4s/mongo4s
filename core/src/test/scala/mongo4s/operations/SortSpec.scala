package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

object SortSpec:
  final case class User(firstName: String, age: Int)

final class SortSpec extends AnyWordSpec, Matchers:
  import SortSpec.User

  private val nameField = Field.of[User, String](_.firstName)
  private val ageField  = Field.of[User, Int](_.age)

  private def json(sort: Sort[User]): String = sort.toBson(FieldNaming.identity).toJson

  "sort keys" should {
    "render direction per field, in declaration order" in {
      json(Sort.asc(nameField).desc(ageField)) shouldBe """{"firstName": 1, "age": -1}"""
    }

    "route derived names through the naming policy" in {
      Sort.asc(nameField).toBson(FieldNaming.snakeCase).toJson shouldBe """{"first_name": 1}"""
    }

    "let the last mention of a field win its priority, not just its direction" in {
      json(Sort.asc(nameField).asc(ageField).desc(nameField)) shouldBe """{"age": 1, "firstName": -1}"""
    }
  }
