package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, BsonInt32, BsonNull, BsonReader, BsonString, BsonWriter}

import mongo4s.bson.{BsonError, BsonTypeName}

object MappedShapeSpec:

  final case class Square(side: Int)

  object Square:
    given WireCodec[Square] =
      WireCodec.derived[Square].iemap(square => Either.cond(square.side > 0, square, "side must be positive"))(identity)

  sealed trait Shape derives WireCodec

  object Shape:
    final case class Circle(radius: Int)   extends Shape derives WireCodec
    final case class Boxed(square: Square) extends Shape derives WireCodec
    final case class Plain(side: Int)      extends Shape

    given WireCodec[Plain] = WireCodec.derived[Plain].imap(identity)(identity)

  final case class Either1(value: Either[Square, Shape.Circle]) derives WireCodec

  sealed trait Legacy derives WireCodec

  object Legacy:
    final case class Nested(side: Int) extends Legacy

    given WireCodec[Nested] =
      val mapped = WireCodec.derived[Nested].imap(identity)(identity)
      WireCodec.from(mapped, mapped)

  final class Nullable(val raw: String | Null)

  object Nullable:
    given WireCodec[Nullable] = WireCodec.instance(
      (writer: BsonWriter, value: Nullable) => if value.raw == null then writer.writeNull() else writer.writeString(value.raw.nn),
      (reader: BsonReader) =>
        if reader.getCurrentBsonType == org.bson.BsonType.NULL
        then
          reader.readNull()
          null
        else Nullable(reader.readString()),
    )

  final case class WithNullable(name: String, raw: Nullable) derives WireCodec

final class MappedShapeSpec extends AnyWordSpec, Matchers:
  import MappedShapeSpec.*

  private def encode[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    val document = BsonDocument()
    codec.encode(BsonDocumentWriter(document), value)
    document

  private def decode[A](document: BsonDocument)(using codec: WireCodec[A]): A = codec.decode(BsonDocumentReader(document))

  "a derived product mapped with imap or iemap" should {

    "keep its fields inline as a case of a sum, the way it was stored before it was mapped" in {
      encode[Shape](Shape.Plain(3)) shouldBe BsonDocument("_type", BsonString("Plain")).append("side", BsonInt32(3))
      decode[Shape](BsonDocument("_type", BsonString("Plain")).append("side", BsonInt32(3))) shouldBe Shape.Plain(3)
    }

    "keep its fields inline as a branch of an Either" in {
      encode(Either1(Left(Square(2)))) shouldBe
        BsonDocument("value", BsonDocument("_type", BsonString("Square")).append("side", BsonInt32(2)))
    }

    "keep the nested shape already written by such a codec when it is wrapped in WireCodec.from" in {
      val nested = BsonDocument("_type", BsonString("Nested")).append("value", BsonDocument("side", BsonInt32(4)))

      encode[Legacy](Legacy.Nested(4)) shouldBe nested
      decode[Legacy](nested) shouldBe Legacy.Nested(4)
    }

    "still validate what it reads" in {
      intercept[BsonError.DecodingFailure](decode[Square](BsonDocument("side", BsonInt32(0)))).error shouldBe
        BsonError.InvalidValue("side must be positive")
    }
  }

  "a field whose codec reads null as a value" should {

    "be read as present, not reported missing" in {
      decode[WithNullable](BsonDocument("name", BsonString("n")).append("raw", BsonNull.VALUE)).raw shouldBe null
    }

    "still be reported missing when it is absent" in {
      intercept[BsonError.DecodingFailure](decode[WithNullable](BsonDocument("name", BsonString("n")))).error shouldBe
        BsonError.MissingField("raw")
    }
  }

  "a discriminator of the wrong type" should {

    "be a type mismatch on _type, not the reader's own exception" in {
      intercept[BsonError.DecodingFailure](decode[Shape](BsonDocument("_type", BsonInt32(1)).append("radius", BsonInt32(1)))).error shouldBe
        BsonError.Nested("_type", BsonError.TypeMismatch(BsonTypeName.String, BsonTypeName.Int))
      intercept[BsonError.DecodingFailure](decode[Shape](BsonDocument("radius", BsonInt32(1)).append("_type", BsonInt32(1)))).error shouldBe
        BsonError.Nested("_type", BsonError.TypeMismatch(BsonTypeName.String, BsonTypeName.Int))
    }
  }
