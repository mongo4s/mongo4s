package mongo4s.bson.catsdata

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, ByteBufNIO}
import cats.data.{Chain, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}

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
