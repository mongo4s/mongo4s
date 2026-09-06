package mongo4s.bson.direct

import java.nio.ByteBuffer
import java.util.UUID
import java.time.Instant

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.*
import org.bson.types.{Decimal128, ObjectId}
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError, BsonTypeName}
import mongo4s.bson.BsonInstances.given

object ScalarWireCodecParitySpec:

  final case class Audit(id: ObjectId, session: UUID, at: Instant, amount: BigDecimal) derives WireCodec

final class ScalarWireCodecParitySpec extends AnyWordSpec, Matchers:
  import ScalarWireCodecParitySpec.*

  private val id      = ObjectId("656b1f2c8e4a3b0012ab34cd")
  private val session = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
  private val at      = Instant.ofEpochMilli(1_733_000_000_123L)
  private val amount  = BigDecimal("12345.6789")

  private val audit = Audit(id, session, at, amount)

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

  private def decodeAudit(document: BsonDocument): Audit =
    WireCodec[Audit].decode(readerOf(bytesOfDocument(document)))

  "the wire path for ObjectId, UUID, Instant and BigDecimal" should {
    "resolve to a ScalarWireCodec instead of the BsonValue fallback" in {
      WireCodec[ObjectId].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[UUID].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[Instant].isInstanceOf[ScalarWireCodec[?]] shouldBe true
      WireCodec[BigDecimal].isInstanceOf[ScalarWireCodec[?]] shouldBe true
    }

    "write exactly what the BsonValue path writes" in {
      val expected = BsonDocument()
        .append("id", BsonEncoder[ObjectId].encode(id))
        .append("session", BsonEncoder[UUID].encode(session))
        .append("at", BsonEncoder[Instant].encode(at))
        .append("amount", BsonEncoder[BigDecimal].encode(amount))

      documentOf(audit) shouldBe expected
    }

    "round-trip through the wire codec" in {
      decodeAudit(documentOf(audit)) shouldBe audit
    }

    "keep the BSON types the server indexes on" in {
      val document = documentOf(audit)

      document.get("id").getBsonType shouldBe BsonType.OBJECT_ID
      document.get("session").getBsonType shouldBe BsonType.STRING
      document.get("at").getBsonType shouldBe BsonType.DATE_TIME
      document.get("amount").getBsonType shouldBe BsonType.DECIMAL128
    }
  }

  "wire decoding of Instant" should {
    "read a timestamp the way the BsonValue path reads it" in {
      val stored   = BsonTimestamp(1_733_000_000, 1)
      val document = documentOf(audit)
      document.put("at", stored)

      decodeAudit(document).at shouldBe BsonDecoder[Instant].decode(stored).toOption.get
    }

    "reject a value the BsonValue path also rejects" in {
      val stored   = BsonString("2026-09-06T00:00:00Z")
      val document = documentOf(audit)
      document.put("at", stored)

      BsonDecoder[Instant].decode(stored).isLeft shouldBe true

      intercept[BsonError.DecodingFailure](decodeAudit(document)).error shouldBe
        BsonError.TypeMismatch(BsonTypeName.Date, BsonTypeName.String)
    }
  }

  "wire decoding of BigDecimal" should {
    "read the numeric types the BsonValue path reads" in {
      val stored = List(BsonInt32(7), BsonInt64(7L), BsonDouble(7.5), BsonDecimal128(Decimal128(java.math.BigDecimal("7.5"))))

      stored.foreach { value =>
        val document = documentOf(audit)
        document.put("amount", value)

        decodeAudit(document).amount shouldBe BsonDecoder[BigDecimal].decode(value).toOption.get
      }
    }
  }

  "wire decoding of UUID" should {
    "reject a malformed value with the same reason as the BsonValue path" in {
      val stored   = BsonString("not-a-uuid")
      val document = documentOf(audit)
      document.put("session", stored)

      val viaValue = BsonDecoder[UUID].decode(stored).left.toOption.get
      val viaWire  = intercept[BsonError.DecodingFailure](decodeAudit(document)).error

      viaWire shouldBe viaValue
    }
  }
