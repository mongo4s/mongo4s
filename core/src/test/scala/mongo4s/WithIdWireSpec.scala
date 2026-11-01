package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.types.ObjectId
import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, BsonInt32, BsonObjectId, BsonString}

import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec, WireCodecConfig}
import mongo4s.bson.{BsonDocumentCodec, BsonError}

object WithIdWireSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class LoginEvent(userId: String, attempts: Int) derives WireCodec

  final case class Note(text: String) derives WireCodec

  object Note:
    given BsonDocumentCodec[Note] = DocumentCodecBridge.toDocumentCodec[Note]

  sealed trait Shape derives WireCodec

  object Shape:
    final case class Circle(radius: Int) extends Shape derives WireCodec

final class WithIdWireSpec extends AnyWordSpec, Matchers:
  import WithIdWireSpec.*

  private def encode[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    val document = BsonDocument()
    codec.encode(BsonDocumentWriter(document), value)
    document

  private def decode[A](document: BsonDocument)(using codec: WireCodec[A]): A = codec.decode(BsonDocumentReader(document))

  private val id = ObjectId("65a1b2c3d4e5f60718293a4b")

  "a WireCodec for WithId" should {

    "write _id first and the entity's fields beside it, under the entity's naming" in {
      val stored = BsonDocument("_id", BsonObjectId(id)).append("user_id", BsonString("u1")).append("attempts", BsonInt32(2))

      encode(WithId(id, LoginEvent("u1", 2))).keySet.toArray.toList shouldBe List("_id", "user_id", "attempts")
      encode(WithId(id, LoginEvent("u1", 2))) shouldBe stored
      decode[WithId[ObjectId, LoginEvent]](stored) shouldBe WithId(id, LoginEvent("u1", 2))
      WireCodec[WithId[ObjectId, LoginEvent]].fieldNaming("userId") shouldBe "user_id"
    }

    "read _id wherever the document has it" in {
      val stored = BsonDocument("user_id", BsonString("u1")).append("_id", BsonObjectId(id)).append("attempts", BsonInt32(2))

      decode[WithId[ObjectId, LoginEvent]](stored) shouldBe WithId(id, LoginEvent("u1", 2))
    }

    "report a document with no _id" in {
      intercept[BsonError.DecodingFailure](
        decode[WithId[ObjectId, LoginEvent]](BsonDocument("user_id", BsonString("u1")).append("attempts", BsonInt32(2)))
      ).error shouldBe
        BsonError.MissingField("_id")
    }

    "resolve without ambiguity for an entity that also has a BsonDocumentCodec" in {
      encode(WithId(id, Note("hi"))).keySet.toArray.toList shouldBe List("_id", "text")
      WireCodec[WithId[ObjectId, Note]].getClass.getName should not include "fromBsonValueCodec"
    }

    "refuse an entity whose codec is not a derived case class" in {
      an[IllegalArgumentException] should be thrownBy WireCodec[WithId[ObjectId, Shape]]
    }
  }
