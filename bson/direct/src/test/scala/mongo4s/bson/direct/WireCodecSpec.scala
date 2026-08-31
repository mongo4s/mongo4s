package mongo4s.bson.direct

import java.nio.ByteBuffer

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.*
import org.bson.types.ObjectId
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonError, FieldNaming}

import mongo4s.bson.BsonInstances.given

object WireCodecSpec:

  object SnakeCaseFields:
    given WireCodecConfig = WireCodecConfig.SnakeCase
    final case class UserAccount(userId: String, isActive: Boolean) derives WireCodec

  object DiscriminatorOnlyMapped:
    given WireCodecConfig = WireCodecConfig.Default.withDiscriminatorNaming(FieldNaming.snakeCase)
    sealed trait Event derives WireCodec
    object Event:
      final case class UserCreated(userId: String) extends Event derives WireCodec

  object FieldsOnlyMapped:
    given WireCodecConfig = WireCodecConfig.SnakeCase
    sealed trait Status derives WireCodec
    object Status:
      final case class ActiveStatus(sinceDay: Int) extends Status derives WireCodec

  object DuplicateFieldNames:
    given WireCodecConfig = WireCodecConfig.Default.withFieldNaming(_ => "same")
    final case class TwoFields(a: Int, b: Int) derives WireCodec

  object EmptyCasesAsString:
    given WireCodecConfig = WireCodecConfig.Default.withEncodeEmptyCasesAsString(true)
    sealed trait Role derives WireCodec
    object Role:
      case object Admin                      extends Role derives WireCodec
      case object Guest                      extends Role derives WireCodec
      final case class Custom(label: String) extends Role derives WireCodec

    final case class Holder(role: Role) derives WireCodec

  object NullsForAbsentOptions:
    given WireCodecConfig = WireCodecConfig.Default.withOmitNoneFields(false)
    final case class LegacyContact(name: String, email: Option[String]) derives WireCodec

  final case class Contact(name: String, email: Option[String], phone: Option[String]) derives WireCodec

  final case class Slots(values: List[Option[Int]]) derives WireCodec

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

  private def documentOf[A](value: A)(using WireCodec[A]): BsonDocument =
    DriverBsonDocumentCodec().decode(readerOf(bytesOf(value)), DecoderContext.builder().build())

  "WireCodec derivation" should {

    "round-trip primitives, a populated Option, List and nested case classes" in {
      val person = Person("1", "bob", 30, 9.5, active = true, List("a", "b"), Some("bobby"), Address("NYC", "10001"))
      roundTrip(person) shouldBe person
    }

    "round-trip an absent Option field and an empty List" in {
      val person = Person("1", "bob", 30, 9.5, active = true, Nil, None, Address("NYC", "10001"))
      roundTrip(person) shouldBe person
    }

    "leave a None field out of the document entirely rather than storing an explicit null" in {
      val document = documentOf(Contact("bob", None, None))

      document.containsKey("name") shouldBe true
      document.containsKey("email") shouldBe false
      document.containsKey("phone") shouldBe false
    }

    "store an explicit null for a None field when omitNoneFields is disabled" in {
      import NullsForAbsentOptions.*

      val document = documentOf(LegacyContact("bob", None))

      document.isNull("email") shouldBe true
    }

    "decode a document carrying an explicit null into None, so documents written before omitNoneFields still read back" in {
      val document = BsonDocument()
        .append("name", BsonString("bob"))
        .append("email", BsonNull.VALUE)
        .append("phone", BsonNull.VALUE)

      WireCodec[Contact].decode(readerOf(bytesOfDocument(document))) shouldBe Contact("bob", None, None)
    }

    "still write a null for a None inside an array, where omitting the element would shift every position after it" in {
      val slots    = Slots(List(Some(1), None, Some(3)))
      val document = documentOf(slots)

      document.getArray("values").size shouldBe 3
      document.getArray("values").get(1).isNull shouldBe true
      roundTrip(slots) shouldBe slots
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

    "apply a WireCodecConfig.fieldNaming strategy to produce snake_case field names on the wire" in {
      val account  = SnakeCaseFields.UserAccount("u1", isActive = true)
      val document = documentOf(account)

      document.keySet should contain allOf ("user_id", "is_active")
      roundTrip(account) shouldBe account
    }

    "map the discriminator independently: only discriminatorNaming set, fields untouched" in {
      import DiscriminatorOnlyMapped.Event
      val event    = Event.UserCreated("u1")
      val document = documentOf[Event](event)

      document.getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "user_created"
      document.containsKey("userId") shouldBe true
      roundTrip[Event](event) shouldBe event
    }

    "map fields independently: only fieldNaming set, discriminator untouched" in {
      import FieldsOnlyMapped.Status
      val status   = Status.ActiveStatus(5)
      val document = documentOf[Status](status)

      document.getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "ActiveStatus"
      document.containsKey("since_day") shouldBe true
      roundTrip[Status](status) shouldBe status
    }

    "throw when a fieldNaming strategy collides two field names" in {
      intercept[IllegalArgumentException](WireCodec[DuplicateFieldNames.TwoFields])
    }

    "encode an empty case (case object), nested in a field, as a bare discriminator string when encodeEmptyCasesAsString is enabled" in {
      import EmptyCasesAsString.{Holder, Role}

      val document = documentOf(Holder(Role.Admin))
      document.getString("role").getValue shouldBe "Admin"

      roundTrip(Holder(Role.Admin: Role)) shouldBe Holder(Role.Admin)
      roundTrip(Holder(Role.Guest: Role)) shouldBe Holder(Role.Guest)
    }

    "still wrap a non-empty case in a document when encodeEmptyCasesAsString is enabled" in {
      import EmptyCasesAsString.{Holder, Role}

      val holder   = Holder(Role.Custom("hi"))
      val document = documentOf(holder)
      document.get("role").asDocument.getString(WireSumDerivation.DiscriminatorField).getValue shouldBe "Custom"
      roundTrip(holder) shouldBe holder
    }
  }
