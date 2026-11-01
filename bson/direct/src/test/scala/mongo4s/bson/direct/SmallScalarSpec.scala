package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.*
import org.bson.types.Decimal128
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonDecoder, BsonEncoder}
import mongo4s.bson.BsonInstances.given

object SmallScalarSpec:

  final case class Reading(ratio: Float, floor: Short, flags: Byte, grade: Char, payload: Array[Byte]) derives WireCodec

  final case class Holder[A](v: A)

  object Holder:
    given [A] => (inner: WireCodec[A]) => WireCodec[Holder[A]] = WireCodec.derived

final class SmallScalarSpec extends AnyWordSpec, Matchers:
  import SmallScalarSpec.*

  private def bytesOf[A](value: A)(using codec: WireCodec[A]): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()
    buffer.toByteArray

  private def bytesOfDocument(document: BsonDocument): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    DriverBsonDocumentCodec().encode(writer, document, EncoderContext.builder().build())
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonBinaryReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private def documentOf[A](value: A)(using WireCodec[A]): BsonDocument =
    DriverBsonDocumentCodec().decode(readerOf(bytesOf(value)), DecoderContext.builder().build())

  private def throughWire[A](stored: BsonValue)(using codec: WireCodec[A]): Either[String, A] =
    try Right(WireCodec[Holder[A]].decode(readerOf(bytesOfDocument(BsonDocument("v", stored)))).v)
    catch case error: Throwable => Left(error.getMessage)

  private def throughValue[A](stored: BsonValue)(using decoder: BsonDecoder[A]): Either[String, A] =
    decoder.decode(stored).left.map(_.message)

  private def agree[A: {WireCodec, BsonDecoder}](stored: BsonValue): org.scalatest.Assertion =
    throughWire[A](stored) shouldBe throughValue[A](stored).left.map(reason => s"v -> $reason")

  private val reading = Reading(0.25f, 12, 3, 'B', Array[Byte](1, 2, 3))

  "Float, Short, Byte, Char and Array[Byte]" should {

    "write the BSON type the Java driver writes for each, on both paths" in {
      val written = documentOf(reading)

      written.get("ratio") shouldBe BsonDouble(0.25)
      written.get("floor") shouldBe BsonInt32(12)
      written.get("flags") shouldBe BsonInt32(3)
      written.get("grade") shouldBe BsonString("B")
      written.get("payload") shouldBe BsonBinary(Array[Byte](1, 2, 3))

      BsonEncoder[Float].encode(0.25f) shouldBe BsonDouble(0.25)
      BsonEncoder[Short].encode(12.toShort) shouldBe BsonInt32(12)
      BsonEncoder[Byte].encode(3.toByte) shouldBe BsonInt32(3)
      BsonEncoder[Char].encode('B') shouldBe BsonString("B")
      BsonEncoder[Array[Byte]].encode(Array[Byte](1, 2, 3)) shouldBe BsonBinary(Array[Byte](1, 2, 3))
    }

    "read back what they wrote" in {
      val read = WireCodec[Reading].decode(readerOf(bytesOf(reading)))

      (read.ratio, read.floor, read.flags, read.grade, read.payload.toList) shouldBe (0.25f, 12.toShort, 3.toByte, 'B', List[Byte](1, 2, 3))
    }

    "be scalars on the wire path, Array[Byte] a binary rather than an array of numbers" in {
      WireCodec[Float].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[Short].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[Byte].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[Char].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[Array[Byte]].isInstanceOf[ScalarWireCodec[?]] shouldBe true
    }
  }

  "reading them" should {
    val numbers = List(BsonInt32(7), BsonInt64(7L), BsonDouble(7.0), BsonDecimal128(Decimal128(java.math.BigDecimal("7"))))

    "accept every width the other path accepts, with the same value" in {
      numbers.foreach { stored =>
        agree[Float](stored)
        agree[Short](stored)
        agree[Byte](stored)
      }
    }

    "refuse what the other path refuses, for the same reason" in {
      List(BsonInt32(40000), BsonDouble(7.5), BsonString("seven")).foreach(agree[Short])
      List(BsonInt32(300), BsonInt32(-129), BsonDouble(1.5)).foreach(agree[Byte])
      List(BsonDouble(1e300), BsonString("x")).foreach(agree[Float])
      List(BsonString("ab"), BsonString(""), BsonInt32(1)).foreach(agree[Char])
      List(BsonString("bytes"), BsonInt32(1)).foreach(agree[Array[Byte]])

      throughValue[Short](BsonInt32(40000)) shouldBe Left("40000 is out of range for Short")
      throughValue[Char](BsonString("ab")) shouldBe Left("\"ab\" is not a single character")
    }

    "keep the edges of each range" in {
      throughValue[Short](BsonInt32(Short.MaxValue)) shouldBe Right(Short.MaxValue)
      throughValue[Byte](BsonInt32(Byte.MinValue)) shouldBe Right(Byte.MinValue)
      throughValue[Float](BsonDouble(Float.MaxValue.toDouble)) shouldBe Right(Float.MaxValue)
      throughValue[Float](BsonDouble(Double.PositiveInfinity)) shouldBe Right(Float.PositiveInfinity)
    }

    "read a binary of any subtype as its bytes" in {
      throughWire[Array[Byte]](BsonBinary(BsonBinarySubType.USER_DEFINED, Array[Byte](9))).map(_.toList) shouldBe Right(List[Byte](9))
      throughValue[Array[Byte]](BsonBinary(BsonBinarySubType.USER_DEFINED, Array[Byte](9))).map(_.toList) shouldBe Right(List[Byte](9))
    }
  }
