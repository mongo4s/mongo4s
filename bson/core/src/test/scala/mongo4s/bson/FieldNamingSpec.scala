package mongo4s.bson

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

final class FieldNamingSpec extends AnyWordSpec, Matchers:

  "identity" should {
    "leave a name alone" in {
      FieldNaming.identity("firstName") shouldBe "firstName"
    }
  }

  "snakeCase" should {
    "split on each capital" in {
      FieldNaming.snakeCase("firstName") shouldBe "first_name"
      FieldNaming.snakeCase("createdAtUtc") shouldBe "created_at_utc"
    }

    "not prefix a leading capital with an underscore" in {
      FieldNaming.snakeCase("Name") shouldBe "name"
    }

    "leave an already-lowercase name alone" in {
      FieldNaming.snakeCase("name") shouldBe "name"
    }

    "split acronyms letter by letter" in {
      FieldNaming.snakeCase("userID") shouldBe "user_i_d"
    }
  }

  "kebabCase" should {
    "behave like snakeCase with hyphens" in {
      FieldNaming.kebabCase("firstName") shouldBe "first-name"
      FieldNaming.kebabCase("Name") shouldBe "name"
    }
  }

  "overrides" should {
    "take the mapped name where one exists" in {
      val naming = FieldNaming.overrides(Map("userID" -> "userId"), fallback = FieldNaming.snakeCase)

      naming("userID") shouldBe "userId"
    }

    "fall back for everything else" in {
      val naming = FieldNaming.overrides(Map("userID" -> "userId"), fallback = FieldNaming.snakeCase)

      naming("firstName") shouldBe "first_name"
    }

    "default its fallback to identity" in {
      FieldNaming.overrides(Map("a" -> "b"))("firstName") shouldBe "firstName"
    }
  }
