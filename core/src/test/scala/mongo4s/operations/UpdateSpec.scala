package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.Field
import mongo4s.bson.{BsonEncoder, FieldNaming}

import mongo4s.bson.BsonInstances.given

object UpdateSpec:
  final case class Note(text: String, rank: Int)
  final case class Person(name: String, age: Int, tags: List[String], score: Option[Long], notes: List[Note])

  object Note:
    given BsonEncoder[Note] = note => BsonDocument().append("text", BsonString(note.text)).append("rank", BsonInt32(note.rank))

final class UpdateSpec extends AnyWordSpec, Matchers:
  import UpdateSpec.{Note, Person}
  import UpdateSpec.Note.given

  // Deliberately suffixed: `tags` alone would shadow AnyWordSpec's own member.
  private val nameField  = Field.of[Person, String](_.name)
  private val ageField   = Field.of[Person, Int](_.age)
  private val tagsField  = Field.of[Person, List[String]](_.tags)
  private val scoreField = Field.of[Person, Option[Long]](_.score)
  private val notesField = Field.of[Person, List[Note]](_.notes)
  private val rankField  = Field.of[Note, Int](_.rank)

  private def json(update: Update[Person]): String = update.toBson(FieldNaming.identity).toJson

  "combining typed operators" should {
    "merge into a single operator document" in {
      json(Update.set(nameField, "bob").and(Update.set(ageField, 30))) shouldBe """{"$set": {"name": "bob", "age": 30}}"""
    }

    "keep distinct operators side by side" in {
      json(Update.set(nameField, "bob").and(Update.inc(ageField, 1))) shouldBe """{"$set": {"name": "bob"}, "$inc": {"age": 1}}"""
    }
  }

  "Update.Raw" should {
    // BsonDocument.append is put, so a raw $set used to replace a typed one outright and drop the
    // typed field with no error at all.
    "merge into an operator that is already present rather than replacing it" in {
      val raw = Update.Raw[Person](BsonDocument("$set", BsonDocument("age", BsonInt32(1))))

      json(Update.set(nameField, "bob").and(raw)) shouldBe """{"$set": {"name": "bob", "age": 1}}"""
    }

    "merge in either order" in {
      val raw = Update.Raw[Person](BsonDocument("$set", BsonDocument("age", BsonInt32(1))))

      json(raw.and(Update.set(nameField, "bob"))) shouldBe """{"$set": {"age": 1, "name": "bob"}}"""
    }

    "carry operators the enum does not model" in {
      val raw = Update.Raw[Person](BsonDocument("$bit", BsonDocument("age", BsonDocument("and", BsonInt32(7)))))

      json(Update.set(nameField, "bob").and(raw)) shouldBe """{"$set": {"name": "bob"}, "$bit": {"age": {"and": 7}}}"""
    }

    "report a shape conflict instead of producing a malformed update" in {
      val raw = Update.Raw[Person](BsonDocument("$set", BsonString("not-a-document")))

      an[IllegalArgumentException] should be thrownBy json(Update.set(nameField, "bob").and(raw))
    }

    // Rendering used to hand the caller's own nested document to the target, so the next typed
    // operator wrote straight into it: the raw value came back permanently carrying fields it never
    // declared, and a second render under a different naming emitted both spellings of the field.
    "leave the caller's document untouched when it is rendered" in {
      val document = BsonDocument("$set", BsonDocument("age", BsonInt32(1)))
      val update   = Update.Raw[Person](document).and(Update.set(nameField, "bob"))

      json(update) shouldBe """{"$set": {"age": 1, "name": "bob"}}"""
      document.toJson shouldBe """{"$set": {"age": 1}}"""
    }

    "render the same document twice under two namings" in {
      val update = Update.Raw[Person](BsonDocument("$set", BsonDocument("age", BsonInt32(1)))).and(Update.set(nameField, "bob"))

      update.toBson(FieldNaming.identity).toJson shouldBe """{"$set": {"age": 1, "name": "bob"}}"""
      update.toBson(FieldNaming.snakeCase).toJson shouldBe """{"$set": {"age": 1, "name": "bob"}}"""
    }

    "stay stable when one raw value is shared by two updates" in {
      val document = BsonDocument("$set", BsonDocument("age", BsonInt32(1)))
      val base     = Update.Raw[Person](document)

      json(base.and(Update.set(nameField, "bob"))) shouldBe """{"$set": {"age": 1, "name": "bob"}}"""
      json(base) shouldBe """{"$set": {"age": 1}}"""
    }
  }

  "an empty update" should {
    "fail with a message naming the problem" in {
      val thrown = the[IllegalArgumentException] thrownBy json(Update.combine())
      thrown.getMessage should include("no operators")
    }
  }

  "array operators" should {
    "accept the field's declared collection type" in {
      json(Update.push(tagsField, "x")) shouldBe """{"$push": {"tags": "x"}}"""
      json(Update.addToSet(tagsField, "x")) shouldBe """{"$addToSet": {"tags": "x"}}"""
      json(Update.pullAll(tagsField, List("x", "y"))) shouldBe """{"$pullAll": {"tags": ["x", "y"]}}"""
    }

    "push many values through $each" in {
      json(Update.pushAll(tagsField, List("x", "y"))) shouldBe """{"$push": {"tags": {"$each": ["x", "y"]}}}"""
    }

    "carry $position, $slice and a scalar $sort alongside $each" in {
      val options = PushOptions.default[String].withPosition(0).withSlice(-10).sortedAscending

      json(Update.pushAll(tagsField, List("x"), options)) shouldBe
        """{"$push": {"tags": {"$each": ["x"], "$position": 0, "$slice": -10, "$sort": 1}}}"""
    }

    "sort pushed documents by one of their own fields" in {
      val options = PushOptions.default[Note].sortedBy(Sort.desc(rankField)).withSlice(3)

      json(Update.pushAll(notesField, List(Note("a", 1)), options)) shouldBe
        """{"$push": {"notes": {"$each": [{"text": "a", "rank": 1}], "$slice": 3, "$sort": {"rank": -1}}}}"""
    }

    "let the last sort choice win, since the two shapes are mutually exclusive" in {
      PushOptions.default[String].sortedAscending.sortedBy(Sort.empty[String]).sortScalars shouldBe None
      PushOptions.default[String].sortedBy(Sort.empty[String]).sortedDescending.sort shouldBe None
    }
  }

  "numeric operators" should {
    "not apply to a non-numeric field" in {
      "Update.inc(nameField, 1)" shouldNot typeCheck
    }

    "render inc, mul, min and max" in {
      json(Update.inc(ageField, 1)) shouldBe """{"$inc": {"age": 1}}"""
      json(Update.mul(ageField, 2)) shouldBe """{"$mul": {"age": 2}}"""
      json(Update.min(ageField, 0)) shouldBe """{"$min": {"age": 0}}"""
      json(Update.max(ageField, 150)) shouldBe """{"$max": {"age": 150}}"""
    }

    // An Option field takes the unwrapped value. There is deliberately no NumericValue[Option[A]]:
    // None has no numeric encoding, and $inc by zero is a write that quietly does nothing.
    "take the unwrapped value for an optional field" in {
      json(Update.inc(scoreField, 5L)) shouldBe """{"$inc": {"score": 5}}"""
      json(scoreField.max(100L)) shouldBe """{"$max": {"score": 100}}"""
    }

    "not accept an Option as the amount" in {
      "Update.inc(scoreField, Some(5L))" shouldNot typeCheck
      "Update.inc(scoreField, Option.empty[Long])" shouldNot typeCheck
    }

    "render setOnInsert, which merge-style upserts need" in {
      json(Update.setOnInsert(nameField, "bob")) shouldBe """{"$setOnInsert": {"name": "bob"}}"""
    }
  }

  "naming" should {
    "apply to derived field names" in {
      Update.set(nameField, "bob").toBson(FieldNaming.snakeCase).toJson shouldBe """{"$set": {"name": "bob"}}"""
      Update.inc(ageField, 1).toBson(FieldNaming.snakeCase).toJson shouldBe """{"$inc": {"age": 1}}"""
    }
  }
