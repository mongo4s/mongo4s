package mongo4s.bson.direct

import java.nio.ByteBuffer

import scala.compiletime.testing.{typeCheckErrors, typeChecks}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.*
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}

object WireCodecDerivationSpec:

  final case class Unmodelled(x: Int)

  final case class Modelled(x: Int) derives WireCodec
  final case class Holder(inner: Modelled, name: String) derives WireCodec

  opaque type UserId = String
  object UserId:
    def apply(raw: String): UserId           = raw
    extension (id: UserId) def value: String = id
    given WireCodec[UserId]                  = ScalarWireCodec[String].imap(apply)(_.value)

  final case class Account(id: UserId, name: String) derives WireCodec

  enum Shape derives WireCodec:
    case Circle(radius: Double)
    case Square(side: Double)

  sealed trait Event derives WireCodec
  object Event:
    final case class Started(at: String) extends Event
    final case class Stopped(at: String) extends Event

final class WireCodecDerivationSpec extends AnyWordSpec, Matchers:
  import WireCodecDerivationSpec.*

  private def roundTrip[A](value: A)(using codec: WireCodec[A]): A =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()
    codec.decode(BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray)))))

  "derivation" should {

    "refuse a type that was never given a codec" in {
      typeChecks("summon[WireCodec[WireCodecDerivationSpec.Unmodelled]]") shouldBe false
    }

    "refuse a product whose field has no codec, rather than deriving the field too" in {
      typeChecks(
        "case class Outer(i: WireCodecDerivationSpec.Unmodelled) derives WireCodec"
      ) shouldBe false
    }

    "accept a product whose fields all have one" in {
      roundTrip(Holder(Modelled(1), "a")) shouldBe Holder(Modelled(1), "a")
    }

    "name the type it is missing, and say how to supply it" in {
      val message = typeCheckErrors(
        "case class Outer(i: WireCodecDerivationSpec.Unmodelled) derives WireCodec"
      ).map(_.message).mkString

      message should include("No WireCodec[")
      message should include("Unmodelled")
      message should include("derives WireCodec")
    }
  }

  "an opaque type" should {

    "not be derived from the type it wraps" in {
      typeChecks("summon[WireCodec[WireCodecDerivationSpec.UserId]]") shouldBe true
      typeChecks("summon[WireCodec[Int]]") shouldBe true
    }

    "round-trip through the codec its owner wrote" in {
      roundTrip(Account(UserId("u-1"), "bob")) shouldBe Account(UserId("u-1"), "bob")
    }
  }

  "a sum" should {

    "derive the cases of an enum, which cannot carry `derives` themselves" in {
      roundTrip[Shape](Shape.Circle(1.5)) shouldBe Shape.Circle(1.5)
      roundTrip[Shape](Shape.Square(2.0)) shouldBe Shape.Square(2.0)
    }

    "derive the leaves of a sealed trait that did not declare their own" in {
      roundTrip[Event](Event.Started("t0")) shouldBe Event.Started("t0")
    }
  }
