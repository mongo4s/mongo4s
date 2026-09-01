package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.types.ObjectId
import org.bson.{BsonDocument, BsonString}

import mongo4s.bson.*

import mongo4s.bson.BsonInstances.given

object WithIdSpec:
  final case class Note(text: String)

final class WithIdSpec extends AnyWordSpec with Matchers:
  import WithIdSpec.Note

  given BsonDocumentCodec[Note] = BsonDocumentCodec.make(
    note => BsonDocument("text", BsonString(note.text)),
    document => BsonDecoder[String].decode(document.get("text")).map(Note(_)),
  )

  "WithId codec" should {
    "round-trip an entity together with its _id" in {
      val oid   = ObjectId.get()
      val value = WithId(oid, Note("hello"))
      val codec = BsonDocumentCodec[WithId[ObjectId, Note]]

      val document = codec.encodeDocument(value)
      document.getObjectId("_id").getValue shouldBe oid
      document.getString("text").getValue shouldBe "hello"

      codec.decodeDocument(document) shouldBe Right(value)
    }

    "fail when _id is absent" in {
      val document = BsonDocument("text", BsonString("hello"))
      BsonDocumentCodec[WithId[ObjectId, Note]].decodeDocument(document) shouldBe
        Left(BsonError.MissingField("_id"))
    }
  }

  "KeyRef.objectId" should {
    "build an _id equality filter" in {
      val oid = ObjectId.get()
      KeyRef.objectId[Note].eqFilter(oid).toBson(FieldNaming.snakeCase).toJson shouldBe
        s"""{"_id": {"$$oid": "${oid.toHexString}"}}"""
    }
  }

  "PrimaryKey.storedId" should {
    "extract the id from the entity and target _id" in {
      final case class Doc(id: ObjectId, text: String)
      val pk  = PrimaryKey.storedId[Doc, ObjectId](_.id)
      val oid = ObjectId.get()

      pk.key(Doc(oid, "x")) shouldBe oid
      pk.eqFilter(oid).toBson(FieldNaming.identity).toJson shouldBe s"""{"_id": {"$$oid": "${oid.toHexString}"}}"""
    }

    "work for a non-ObjectId key" in {
      final case class Doc(slug: String)
      val pk = PrimaryKey.storedId[Doc, String](_.slug)

      pk.fieldNames shouldBe List("_id")
      pk.eqFilter("intro").toBson(FieldNaming.snakeCase).toJson shouldBe """{"_id": "intro"}"""
    }
  }
