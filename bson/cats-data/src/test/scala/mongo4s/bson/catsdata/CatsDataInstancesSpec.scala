package mongo4s.bson.catsdata

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, ByteBufNIO}
import cats.data.{Chain, Ior, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}

import mongo4s.bson.direct.WireCodec
import mongo4s.bson.{BsonDecoder, BsonEncoder}

import mongo4s.bson.BsonInstances.given
import CatsDataBsonInstances.given
import CatsDataWireInstances.given

final class CatsDataInstancesSpec extends AnyWordSpec, Matchers:

  private def bsonRoundTrip[A](value: A)(using enc: BsonEncoder[A], dec: BsonDecoder[A]): Either[?, A] =
    dec.decode(enc.encode(value))

  private def wireRoundTrip[A](value: A)(using codec: WireCodec[A]): A =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    writer.writeStartDocument()
    writer.writeName("value")
    codec.encode(writer, value)
    writer.writeEndDocument()
    writer.flush()

    val reader = BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
    reader.readStartDocument()
    reader.readBsonType()
    reader.readName()
    val result = codec.decode(reader)
    reader.readEndDocument()
    result

  // Ior's WireCodec is document-shaped (always writes its own {"_type": ..., ...} document), unlike the
  // array-shaped NonEmptyList/etc above, so it can be encoded standalone at the wire root without the extra
  // "value" field wrapper wireRoundTrip needs for those.
  private def iorRoundTrip[A](value: A)(using codec: WireCodec[A]): A =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()

    val reader = BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
    codec.decode(reader)

  "cats.data BsonEncoder/BsonDecoder instances" should {
    "round-trip NonEmptyList" in {
      val value = NonEmptyList.of(1, 2, 3)
      bsonRoundTrip(value) shouldBe Right(value)
    }
    "round-trip Chain, including an empty one" in {
      bsonRoundTrip(Chain(1, 2, 3)) shouldBe Right(Chain(1, 2, 3))
      bsonRoundTrip(Chain.empty[Int]) shouldBe Right(Chain.empty[Int])
    }
    "round-trip NonEmptyVector" in {
      val value = NonEmptyVector.of(1, 2, 3)
      bsonRoundTrip(value) shouldBe Right(value)
    }
    "round-trip NonEmptySet" in {
      val value = NonEmptySet.of(1, 2, 3)
      bsonRoundTrip(value) shouldBe Right(value)
    }
    "round-trip NonEmptyMap" in {
      val value = NonEmptyMap.of("a" -> 1, "b" -> 2)
      bsonRoundTrip(value) shouldBe Right(value)
    }
  }

  "cats.data Ior BsonEncoder/BsonDecoder instances" should {
    "round-trip Left/Right/Both" in {
      bsonRoundTrip[Ior[String, Int]](Ior.Left("boom")) shouldBe Right(Ior.Left("boom"))
      bsonRoundTrip[Ior[String, Int]](Ior.Right(1)) shouldBe Right(Ior.Right(1))
      bsonRoundTrip[Ior[String, Int]](Ior.Both("warn", 2)) shouldBe Right(Ior.Both("warn", 2))
    }

    "discriminate by each branch's own type name, and tag Both with both names combined" in {
      // ClassTag[Int].runtimeClass is the JVM primitive class, so its simple name is "int" (lowercase),
      // not "Integer" — a real quirk of the ClassTag-based discriminator worth knowing about, not a bug.
      val right = BsonEncoder[Ior[String, Int]].encode(Ior.Right(1)).asDocument
      right.getString("_type").getValue shouldBe "int"
      right.getInt32("value").getValue shouldBe 1

      val both = BsonEncoder[Ior[String, Int]].encode(Ior.Both("warn", 2)).asDocument
      both.getString("_type").getValue shouldBe "String+int"
      both.getString("left").getValue shouldBe "warn"
      both.getInt32("right").getValue shouldBe 2
    }

    "refuse to derive when both type parameters share a type name" in {
      intercept[IllegalArgumentException] {
        summon[BsonEncoder[Ior[Int, Int]]]
      }
      intercept[IllegalArgumentException] {
        summon[BsonDecoder[Ior[Int, Int]]]
      }
    }
  }

  "cats.data WireCodec instances" should {
    "round-trip NonEmptyList" in {
      val value = NonEmptyList.of("a", "b", "c")
      wireRoundTrip(value) shouldBe value
    }
    "round-trip Chain, including an empty one" in {
      wireRoundTrip(Chain("a", "b")) shouldBe Chain("a", "b")
      wireRoundTrip(Chain.empty[String]) shouldBe Chain.empty[String]
    }
    "round-trip NonEmptyVector" in {
      val value = NonEmptyVector.of("a", "b", "c")
      wireRoundTrip(value) shouldBe value
    }
    "round-trip NonEmptySet" in {
      val value = NonEmptySet.of(1, 2, 3)
      wireRoundTrip(value) shouldBe value
    }
    "round-trip NonEmptyMap" in {
      val value = NonEmptyMap.of("a" -> 1, "b" -> 2)
      wireRoundTrip(value) shouldBe value
    }
  }

  "cats.data Ior WireCodec instance" should {
    final case class Foo(x: Int) derives WireCodec

    "round-trip Left/Right/Both" in {
      iorRoundTrip[Ior[String, Foo]](Ior.Left("boom")) shouldBe Ior.Left("boom")
      iorRoundTrip[Ior[String, Foo]](Ior.Right(Foo(1))) shouldBe Ior.Right(Foo(1))
      iorRoundTrip[Ior[String, Foo]](Ior.Both("warn", Foo(2))) shouldBe Ior.Both("warn", Foo(2))
    }

    "discriminate by each branch's own type name, not Left/Right/Both" in {
      val buffer = BasicOutputBuffer()
      val writer = BsonBinaryWriter(buffer)
      WireCodec[Ior[String, Foo]].encode(writer, Ior.Right(Foo(1)))
      writer.flush()

      val reader   = BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
      val document = org.bson.codecs.BsonDocumentCodec().decode(reader, org.bson.codecs.DecoderContext.builder().build())
      document.getString("_type").getValue shouldBe "Foo"
    }

    "tag Both with both branch's type names combined, and nest each side under its own key" in {
      val buffer = BasicOutputBuffer()
      val writer = BsonBinaryWriter(buffer)
      WireCodec[Ior[String, Foo]].encode(writer, Ior.Both("warn", Foo(2)))
      writer.flush()

      val reader   = BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
      val document = org.bson.codecs.BsonDocumentCodec().decode(reader, org.bson.codecs.DecoderContext.builder().build())
      document.getString("_type").getValue shouldBe "String+Foo"
      document.getString("left").getValue shouldBe "warn"
      document.getDocument("right").getInt32("x").getValue shouldBe 2
    }

    "refuse to derive when both type parameters share a type name" in {
      intercept[IllegalArgumentException] {
        summon[WireCodec[Ior[Foo, Foo]]]
      }
    }
  }
