package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.types.ObjectId
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, BsonDouble, BsonString, ByteBufNIO}

import mongo4s.bson.BsonError

import mongo4s.bson.BsonInstances.given

object WireCodecSpec:

  final case class Address(city: String, zip: String) derives WireCodec

  final case class Person(
      id: String,
      name: String,
      age: Int,
      score: Double,
      active: Boolean,
      tags: List[String],
      nickname: Option[String],
      address: Address,
  ) derives WireCodec

  sealed trait Shape derives WireCodec
  object Shape:
    final case class Circle(radius: Double)                   extends Shape derives WireCodec
    final case class Rectangle(width: Double, height: Double) extends Shape derives WireCodec

  final case class WithObjectId(id: ObjectId, note: String) derives WireCodec

  final case class Tree(value: Int, children: List[Tree]) derives WireCodec

  sealed trait Expr derives WireCodec
  object Expr:
    final case class Lit(value: Int)              extends Expr derives WireCodec
    final case class Add(left: Expr, right: Expr) extends Expr derives WireCodec

final class WireCodecSpec extends AnyWordSpec, Matchers:
  import WireCodecSpec.*

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

  "WireCodec derivation" should {

    "round-trip primitives, a populated Option, List and nested case classes" in {
      val person = Person("1", "bob", 30, 9.5, active = true, List("a", "b"), Some("bobby"), Address("NYC", "10001"))
      roundTrip(person) shouldBe person
    }

    "round-trip an absent Option field and an empty List" in {
      val person = Person("1", "bob", 30, 9.5, active = true, Nil, None, Address("NYC", "10001"))
      roundTrip(person) shouldBe person
    }

    "round-trip sealed trait cases via the discriminator field" in {
      roundTrip[Shape](Shape.Circle(2.0)) shouldBe Shape.Circle(2.0)
      roundTrip[Shape](Shape.Rectangle(3.0, 4.0)) shouldBe Shape.Rectangle(3.0, 4.0)
    }

    "bridge scalar types that only have a BsonEncoder/BsonDecoder, via the low-priority fallback" in {
      val value = WithObjectId(ObjectId.get(), "hello")
      roundTrip(value) shouldBe value
    }

    "round-trip a self-referential (recursive) case class" in {
      val tree = Tree(1, List(Tree(2, Nil), Tree(3, List(Tree(4, Nil)))))
      roundTrip(tree) shouldBe tree
    }

    "round-trip a mutually-recursive sealed trait (a case referencing the sum type directly, not through a container)" in {
      val expr: Expr = Expr.Add(Expr.Lit(1), Expr.Add(Expr.Lit(2), Expr.Lit(3)))
      roundTrip(expr) shouldBe expr
    }

    "fail to decode when a required (non-Option) field is missing" in {
      val person   = Person("1", "bob", 30, 9.5, active = true, Nil, None, Address("NYC", "10001"))
      val document = DriverBsonDocumentCodec().decode(readerOf(bytesOf(person)), DecoderContext.builder().build())
      document.remove("age")

      intercept[BsonError.DecodingFailure](WireCodec[Person].decode(readerOf(bytesOfDocument(document))))
    }

    "fail to decode a sealed trait document whose discriminator isn't the first field" in {
      val document = BsonDocument()
        .append("radius", BsonDouble(2.0))
        .append(WireSumDerivation.DiscriminatorField, BsonString("Circle"))

      intercept[BsonError.DecodingFailure](WireCodec[Shape].decode(readerOf(bytesOfDocument(document))))
    }

    "fail to decode a sealed trait document with an unknown discriminator" in {
      val document = BsonDocument().append(WireSumDerivation.DiscriminatorField, BsonString("Triangle"))

      intercept[BsonError.DecodingFailure](WireCodec[Shape].decode(readerOf(bytesOfDocument(document))))
    }
  }
