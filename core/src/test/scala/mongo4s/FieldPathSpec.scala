package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.bson.FieldNaming

final class FieldPathSpec extends AnyWordSpec, Matchers:

  "render" should {
    "spell a single derived segment through the naming policy" in {
      FieldPath.of("firstName").render(FieldNaming.snakeCase) shouldBe "first_name"
      FieldPath.of("firstName").render(FieldNaming.identity) shouldBe "firstName"
    }

    "leave a single stored segment verbatim" in {
      FieldPath.literal("firstName").render(FieldNaming.snakeCase) shouldBe "firstName"
    }

    "join a nested path with dots, deciding each segment on its own" in {
      FieldPath.derived(List("address", "zipCode")).render(FieldNaming.snakeCase) shouldBe "address.zip_code"
    }

    "mix derived and stored segments in one path" in {
      FieldPath.of("totalsByCurrency").stored("EUR").render(FieldNaming.snakeCase) shouldBe "totals_by_currency.EUR"
    }

    "produce an empty string for an empty path" in {
      FieldPath(Nil).render(FieldNaming.snakeCase) shouldBe ""
    }

    "return the segment's own rendering for a single-segment path, without joining" in {
      val naming = FieldNaming.overrides(Map("userID" -> "userId"))

      FieldPath.of("userID").render(naming) shouldBe "userId"
    }
  }

  "literal" should {
    "split a dotted name into stored segments" in {
      FieldPath.literal("a.b").render(FieldNaming.snakeCase) shouldBe "a.b"
    }
  }

  "appending a segment" should {
    "add it as stored, whether through / or stored" in {
      val base = FieldPath.of("tags")

      (base / "0").render(FieldNaming.identity) shouldBe "tags.0"
      base.stored("0").render(FieldNaming.identity) shouldBe "tags.0"
    }

    "leave the original path unchanged" in {
      val base = FieldPath.of("tags")

      base / "0": Unit

      base.render(FieldNaming.identity) shouldBe "tags"
    }
  }

  "equality" should {
    "compare structurally" in {
      FieldPath.derived(List("a", "b")) shouldBe FieldPath.derived(List("a", "b"))
      FieldPath.derived(List("a")) should not be FieldPath.literal("a")
    }
  }

  "the representation" should {
    "stay opaque, so the segment list is not part of the published API" in {
      "val segments: List[FieldPath.Segment] = FieldPath.of(\"a\")" shouldNot typeCheck
      "val path: FieldPath = List(FieldPath.Segment.Derived(\"a\"))" shouldNot typeCheck
      "FieldPath.of(\"a\").segments" shouldNot typeCheck
    }
  }
