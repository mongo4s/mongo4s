package mongo4s.bson.direct

import java.nio.ByteBuffer

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, BsonString, ByteBufNIO}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonError, BsonEncoder}

object ScalarWireCodecSpec:

  final case class EventId(value: String)
  object EventId:
    given ScalarWireCodec[EventId] = ScalarWireCodec[String].imap(EventId.apply)(_.value)
    given BsonEncoder[EventId]     = summon[ScalarWireCodec[EventId]].toBsonEncoder

  enum Provider(val value: String):
    case Stripe extends Provider("stripe")
    case Adyen  extends Provider("adyen")

  object Provider:
    def from(value: String): Option[Provider] = Provider.values.find(_.value == value)

    given ScalarWireCodec[Provider] =
      ScalarWireCodec[String].iemap(raw => from(raw).toRight(s"Unsupported provider: $raw"))(_.value)
    given BsonEncoder[Provider]     = summon[ScalarWireCodec[Provider]].toBsonEncoder

  final case class Address(city: String, zip: String) derives WireCodec

  // ScalarWireCodec values are only ever legal nested inside a document field (a bare scalar can't be
  // written to the root of a real BSON document) — these holders exercise that real usage shape.
  final case class EventIdHolder(id: EventId) derives WireCodec
  final case class ProviderHolder(provider: Provider) derives WireCodec

final class ScalarWireCodecSpec extends AnyWordSpec, Matchers:
  import ScalarWireCodecSpec.*

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

  private def roundTrip[A](value: A)(using codec: WireCodec[A]): A =
    codec.decode(readerOf(bytesOf(value)))

  private def documentOf[A](value: A)(using WireCodec[A]): BsonDocument =
    DriverBsonDocumentCodec().decode(readerOf(bytesOf(value)), DecoderContext.builder().build())

  "ScalarWireCodec.imap" should {
    "round-trip a nested scalar field through WireCodec" in {
      roundTrip(EventIdHolder(EventId("abc123"))) shouldBe EventIdHolder(EventId("abc123"))
    }

    "produce a working BsonEncoder via toBsonEncoder" in {
      BsonEncoder[EventId].encode(EventId("abc123")) shouldBe BsonString("abc123")
    }
  }

  "ScalarWireCodec.iemap" should {
    "round-trip a nested known value through WireCodec" in {
      roundTrip(ProviderHolder(Provider.Stripe)) shouldBe ProviderHolder(Provider.Stripe)
    }

    "fail to decode an unknown value" in {
      val document = documentOf(ProviderHolder(Provider.Stripe))
      document.put("provider", BsonString("unknown"))

      intercept[BsonError.DecodingFailure] {
        WireCodec[ProviderHolder].decode(readerOf(bytesOfDocument(document)))
      }
    }

    "produce a working BsonEncoder that only exercises the encode direction" in {
      BsonEncoder[Provider].encode(Provider.Adyen) shouldBe BsonString("adyen")
    }
  }

  "ScalarWireCodec" should {
    "not be derivable for a document-shaped (derived) case class" in {
      typeChecks("summon[mongo4s.bson.direct.ScalarWireCodec[mongo4s.bson.direct.ScalarWireCodecSpec.Address]]") shouldBe false
      typeChecks("summon[mongo4s.bson.direct.WireCodec[mongo4s.bson.direct.ScalarWireCodecSpec.Address]]") shouldBe true
    }
  }
