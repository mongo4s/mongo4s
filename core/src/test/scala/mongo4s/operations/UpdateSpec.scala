package mongo4s.operations

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.Field
import mongo4s.bson.{BsonEncoder, FieldNaming}

import mongo4s.bson.BsonInstances.given

object UpdateSpec:
  final case class Note(text: String, rank: Int)
  final case class Person(name: String, age: Int, tags: List[String], score: Option[Long], notes: List[Note], lastSeen: java.time.Instant)

  object Note:
    given BsonEncoder[Note] = note => BsonDocument().append("text", BsonString(note.text)).append("rank", BsonInt32(note.rank))

final class UpdateSpec extends AnyWordSpec, Matchers:
  import UpdateSpec.{Note, Person}
  import UpdateSpec.Note.given

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

  "an update that changes one path twice" should {
    "be refused rather than keep only the last change, as one operator document would" in {
      val thrown = the[IllegalArgumentException] thrownBy json(Update.inc(ageField, 1).and(Update.inc(ageField, 2)))
      thrown.getMessage should include("'age' twice")
    }

    "be refused when a raw update repeats a path a typed one already changes" in {
      val raw = Update.Raw[Person](BsonDocument("$set", BsonDocument("name", BsonString("eve"))))
      an[IllegalArgumentException] should be thrownBy json(Update.set(nameField, "bob").and(raw))
    }
  }

  "an update whose paths overlap across operators" should {
    "be refused, as the server refuses it" in {
      the[IllegalArgumentException] thrownBy json(Update.set(nameField, "bob").and(Update.unset(nameField))) should have message
        "Update changes 'name' under two operators — MongoDB refuses that as a conflict"

      an[IllegalArgumentException] should be thrownBy json(Update.unset(notesField).and(Update.set(notesField / "0" / "rank", 1)))
      an[IllegalArgumentException] should be thrownBy json(Update.set(notesField / "0" / "rank", 1).and(Update.unset(notesField)))
      an[IllegalArgumentException] should be thrownBy json(Update.rename(nameField, Field.of[Person, String](_.name)).and(Update.set(nameField, "x")))
    }

    "still accept paths that only share a prefix of their spelling" in {
      val rank  = Field.stored[Person, Int]("notes.rank")
      val ranks = Field.stored[Person, Int]("notes_rank")

      json(Update.set(rank, 1).and(Update.set(ranks, 2))) shouldBe """{"$set": {"notes.rank": 1, "notes_rank": 2}}"""
    }
  }

  "a pipeline update" should {
    val shout = Stage.addFields[Person]("name" := nameField.expr.toUpper)
    val tidy  = Stage.project(Projection.empty[Person].exclude(ageField))

    "render as the stages it holds, and not as an operator document" in {
      Update.pipeline(shout, tidy).toPipeline(FieldNaming.identity).map(_.map(_.toJson)) shouldBe Some(
        List("""{"$addFields": {"name": {"$toUpper": "$name"}}}""", """{"$project": {"age": 0}}""")
      )
      Update.set(nameField, "bob").toPipeline(FieldNaming.identity) shouldBe None
    }

    "join another pipeline into one, but refuse to be mixed with operators" in {
      Update.pipeline(shout).and(Update.pipeline(tidy)) shouldBe Update.pipeline(shout, tidy)
      an[IllegalArgumentException] should be thrownBy json(Update.pipeline(shout).and(Update.set(nameField, "bob")))
    }

    "take $unset, which the server allows in an update alongside $project" in {
      Update.pipeline(Stage.unset(ageField)).toPipeline(FieldNaming.identity).map(_.map(_.toJson)) shouldBe Some(List("""{"$unset": "age"}"""))
    }

    "refuse a stage the server does not take in an update" in {
      val thrown = the[IllegalArgumentException] thrownBy Update.pipeline(Stage.matching(nameField.equalTo("bob")))
      thrown.getMessage should include("$match")
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

  "a numeric update over an Int field" should {

    "write the width the field's own encoder writes" in {
      json(Update.set(ageField, 31)) shouldBe """{"$set": {"age": 31}}"""
      json(Update.inc(ageField, 1)) shouldBe """{"$inc": {"age": 1}}"""
      json(Update.mul(ageField, 2)) shouldBe """{"$mul": {"age": 2}}"""
    }

    "refuse an amount of another numeric type, which would change the stored width" in {
      typeChecks("Update.inc(Field.of[UpdateSpec.Person, Int](_.age), 1)") shouldBe true
      typeChecks("Update.inc(Field.of[UpdateSpec.Person, Int](_.age), 1L)") shouldBe false
      typeChecks("Update.inc(Field.of[UpdateSpec.Person, Int](_.age), 1.5)") shouldBe false
      typeChecks("Update.mul(Field.of[UpdateSpec.Person, Int](_.age), 1.5)") shouldBe false
      typeChecks("Update.set(Field.of[UpdateSpec.Person, Int](_.age), 1L)") shouldBe false
    }

    "refuse a numeric operator on a field that is not numeric" in {
      typeChecks("Update.inc(Field.of[UpdateSpec.Person, String](_.name), 1)") shouldBe false
    }
  }

  "$min and $max" should {

    "apply to every type MongoDB orders, not only to numbers" in {
      typeChecks("Update.max(Field.of[UpdateSpec.Person, Int](_.age), 5)") shouldBe true
      typeChecks("Update.max(Field.of[UpdateSpec.Person, String](_.name), \"z\")") shouldBe true
      typeChecks("Update.max(Field.of[UpdateSpec.Person, java.time.Instant](_.lastSeen), java.time.Instant.now)") shouldBe true
      typeChecks("Update.min(Field.of[UpdateSpec.Person, java.time.Instant](_.lastSeen), java.time.Instant.now)") shouldBe true
    }

    "reach through an Option the way $inc does" in {
      typeChecks("Update.max(Field.of[UpdateSpec.Person, Option[Long]](_.score), 5L)") shouldBe true
    }

    "still refuse a value of another type than the field" in {
      typeChecks("Update.max(Field.of[UpdateSpec.Person, Int](_.age), \"z\")") shouldBe false
      typeChecks("Update.max(Field.of[UpdateSpec.Person, java.time.Instant](_.lastSeen), 5)") shouldBe false
    }

    "render the value the field's own encoder writes" in {
      val at = java.time.Instant.ofEpochMilli(1000)

      json(Update.max(Field.of[UpdateSpec.Person, java.time.Instant](_.lastSeen), at)) shouldBe
        """{"$max": {"lastSeen": {"$date": "1970-01-01T00:00:01Z"}}}"""
    }
  }
