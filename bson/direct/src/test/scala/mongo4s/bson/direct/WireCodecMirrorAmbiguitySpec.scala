package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, ByteBufNIO}

final class WireCodecMirrorAmbiguitySpec extends AnyWordSpec, Matchers:

  final case class Foo(x: Int) derives WireCodec

  final case class ListHolder(foos: List[Foo])     derives WireCodec
  final case class OptionHolder(foo: Option[Foo])  derives WireCodec

  private def documentOf[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()
    val reader = BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
    DriverBsonDocumentCodec().decode(reader, DecoderContext.builder().build())

  "WireCodec.derived alongside the specific collection instances" should {
    "encode List[CaseClass] as a real BSON array, not a ::/Nil document" in {
      val document = documentOf(ListHolder(List(Foo(1), Foo(2))))
      document.isArray("foos") shouldBe true
      document.getArray("foos").size shouldBe 2
    }

    "encode Option[CaseClass] as the bare value/null, not a Some/None document" in {
      val present = documentOf(OptionHolder(Some(Foo(1))))
      present.isDocument("foo") shouldBe true
      present.getDocument("foo").getInt32("x").getValue shouldBe 1

      val absent = documentOf(OptionHolder(None))
      absent.isNull("foo") shouldBe true
    }
  }
