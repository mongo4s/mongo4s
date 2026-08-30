package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, ByteBufNIO}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext}

final class WireCodecMirrorAmbiguitySpec extends AnyWordSpec, Matchers:

  final case class Foo(x: Int) derives WireCodec

  final case class ListHolder(foos: List[Foo]) derives WireCodec
  final case class OptionHolder(foo: Option[Foo]) derives WireCodec
  final case class NestedListHolder(foos: List[List[Foo]]) derives WireCodec
  final case class VectorHolder(foos: Vector[Foo]) derives WireCodec
  final case class SeqHolder(foos: Seq[Foo]) derives WireCodec
  final case class SetHolder(foos: Set[Foo]) derives WireCodec

  final case class EitherHolder(value: Either[String, Foo]) derives WireCodec

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

    "encode nested List[List[CaseClass]] as nested arrays" in {
      val document = documentOf(NestedListHolder(List(List(Foo(1), Foo(2)), List(Foo(3)))))
      document.isArray("foos") shouldBe true
      val outer    = document.getArray("foos")
      outer.size shouldBe 2
      outer.get(0).asArray.size shouldBe 2
      outer.get(1).asArray.size shouldBe 1
    }

    "encode Vector[CaseClass] as a real BSON array" in {
      documentOf(VectorHolder(Vector(Foo(1), Foo(2)))).isArray("foos") shouldBe true
    }

    "encode Seq[CaseClass] as a real BSON array" in {
      documentOf(SeqHolder(Seq(Foo(1), Foo(2)))).isArray("foos") shouldBe true
    }

    "encode Set[CaseClass] as a real BSON array" in {
      documentOf(SetHolder(Set(Foo(1), Foo(2)))).isArray("foos") shouldBe true
    }

    "encode Either[String, CaseClass] via its own dedicated instance, discriminated by type name" in {
      val right = documentOf(EitherHolder(Right(Foo(1))))
      right.getDocument("value").getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "Foo"

      val left = documentOf(EitherHolder(Left("boom")))
      left.getDocument("value").getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "String"
    }
  }
