package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, ByteBufNIO}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext}

import mongo4s.bson.BsonError

object EitherWireCodecSpec:
  final case class Foo(x: Int)  derives WireCodec
  final case class Bar(y: String) derives WireCodec

final class EitherWireCodecSpec extends AnyWordSpec, Matchers:
  import EitherWireCodecSpec.*

  private def bytesOf[A](value: A)(using codec: WireCodec[A]): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonBinaryReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private def roundTrip[A](value: A)(using codec: WireCodec[A]): A =
    codec.decode(readerOf(bytesOf(value)))

  private def documentOf[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    DriverBsonDocumentCodec().decode(readerOf(bytesOf(value)), DecoderContext.builder().build())

  "WireCodec[Either[A, B]]" should {
    "round-trip Left and Right" in {
      roundTrip[Either[String, Foo]](Left("boom")) shouldBe Left("boom")
      roundTrip[Either[String, Foo]](Right(Foo(1))) shouldBe Right(Foo(1))
    }

    "inline a document-shaped (case class) branch's fields directly, not under a 'value' key" in {
      val document = documentOf[Either[String, Foo]](Right(Foo(1)))
      document.getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "Foo"
      document.getInt32("x").getValue shouldBe 1
      document.containsKey("value") shouldBe false
    }

    "wrap a scalar branch's value under a 'value' key" in {
      val document = documentOf[Either[String, Foo]](Left("boom"))
      document.getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "String"
      document.getString("value").getValue shouldBe "boom"
    }

    "discriminate by the branch's own type name, not Left/Right" in {
      documentOf[Either[Bar, Foo]](Left(Bar("hi"))).getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "Bar"
      documentOf[Either[Bar, Foo]](Right(Foo(1))).getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "Foo"
    }

    "fail to decode an unknown discriminator" in {
      val document = documentOf[Either[String, Foo]](Right(Foo(1)))
      document.put(WireSumDerivation.DiscriminatorField, org.bson.BsonString("Unknown"))
      intercept[BsonError.DecodingFailure] {
        WireCodec[Either[String, Foo]].decode(readerOf {
          val buffer = BasicOutputBuffer()
          val writer = BsonBinaryWriter(buffer)
          DriverBsonDocumentCodec().encode(writer, document, org.bson.codecs.EncoderContext.builder().build())
          writer.flush()
          buffer.toByteArray
        })
      }
    }

    "refuse to derive when both branches have the same type name (ambiguous discriminator)" in {
      intercept[IllegalArgumentException] {
        summon[WireCodec[Either[Foo, Foo]]]
      }
    }
  }
