package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, BsonInt32, BsonObjectId, BsonString}

import mongo4s.bson.BsonError

object DiscriminatorConfigSpec:

  object Kind:
    given WireCodecConfig = WireCodecConfig.Default.withDiscriminatorField("kind")

    sealed trait Shape derives WireCodec

    object Shape:
      final case class Circle(radius: Int) extends Shape derives WireCodec
      case object Dot                      extends Shape

    sealed trait Clashing derives WireCodec

    object Clashing:
      final case class Labelled(kind: String) extends Clashing derives WireCodec

  object Wrapped:
    given WireCodecConfig = WireCodecConfig.Default.withWrappedCases(true)

    final case class Score(home: Int, away: Int) derives WireCodec

    sealed trait Item derives WireCodec

    object Item:
      final case class ScoreItem(score: Score) extends Item derives WireCodec
      final case class Labelled(_type: String) extends Item derives WireCodec
      case object Empty                        extends Item

  object Tolerant:
    given WireCodecConfig = WireCodecConfig.Default.withFallbackCase("Unknown")

    sealed trait Event derives WireCodec

    object Event:
      final case class Opened(at: Int) extends Event derives WireCodec
      case object Unknown              extends Event

    sealed trait Strict derives WireCodec

    object Strict:
      final case class Only(at: Int) extends Strict derives WireCodec

    sealed trait Misconfigured derives WireCodec

    object Misconfigured:
      final case class Unknown(at: Int) extends Misconfigured derives WireCodec

  object TolerantWrapped:
    given WireCodecConfig = WireCodecConfig.Default.withWrappedCases(true).withFallbackCase("Unknown")

    sealed trait Event derives WireCodec

    object Event:
      final case class Opened(at: Int) extends Event derives WireCodec
      case object Unknown              extends Event

final class DiscriminatorConfigSpec extends AnyWordSpec, Matchers:
  import DiscriminatorConfigSpec.*

  private def encode[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    val document = BsonDocument()
    codec.encode(BsonDocumentWriter(document), value)
    document

  private def decode[A](document: BsonDocument)(using codec: WireCodec[A]): A = codec.decode(BsonDocumentReader(document))

  "withDiscriminatorField" should {

    "name the case under the field given" in {
      import Kind.Shape

      encode[Shape](Shape.Circle(2)) shouldBe BsonDocument("kind", BsonString("Circle")).append("radius", BsonInt32(2))
      decode[Shape](BsonDocument("radius", BsonInt32(2)).append("kind", BsonString("Circle"))) shouldBe Shape.Circle(2)
      decode[Shape](BsonDocument("kind", BsonString("Dot"))) shouldBe Shape.Dot
    }

    "refuse a case whose own field collides with it" in {
      an[IllegalArgumentException] should be thrownBy encode[Kind.Clashing](Kind.Clashing.Labelled("x"))
    }
  }

  "withWrappedCases" should {

    "write each case as one field named after it, holding the case" in {
      import Wrapped.{Item, Score}

      val wrapped = BsonDocument("ScoreItem", BsonDocument("score", BsonDocument("home", BsonInt32(1)).append("away", BsonInt32(0))))

      encode[Item](Item.ScoreItem(Score(1, 0))) shouldBe wrapped
      decode[Item](wrapped) shouldBe Item.ScoreItem(Score(1, 0))
      encode[Item](Item.Empty) shouldBe BsonDocument("Empty", BsonDocument())
      decode[Item](BsonDocument("Empty", BsonDocument())) shouldBe Item.Empty
    }

    "find the case past the _id the server stores first, and let a case have a field called _type" in {
      import Wrapped.Item

      val stored = BsonDocument("_id", BsonObjectId()).append("Labelled", BsonDocument("_type", BsonString("t")))

      decode[Item](stored) shouldBe Item.Labelled("t")
    }

    "name the case in a failure inside it" in {
      import Wrapped.Item

      intercept[BsonError.DecodingFailure](decode[Item](BsonDocument("ScoreItem", BsonDocument("score", BsonInt32(1))))).getMessage should startWith(
        "ScoreItem -> score ->"
      )
    }
  }

  "withFallbackCase" should {

    "read a subtype it does not know as the fallback case" in {
      import Tolerant.Event

      decode[Event](BsonDocument("_type", BsonString("Closed")).append("at", BsonInt32(3))) shouldBe Event.Unknown
      decode[Event](BsonDocument("_type", BsonString("Opened")).append("at", BsonInt32(3))) shouldBe Event.Opened(3)
    }

    "work on wrapped cases too" in {
      import TolerantWrapped.Event

      decode[Event](BsonDocument("Closed", BsonDocument("at", BsonInt32(3)))) shouldBe Event.Unknown
    }

    "still fail on an unknown subtype of a sum that has no such case" in {
      intercept[BsonError.DecodingFailure](decode[Tolerant.Strict](BsonDocument("_type", BsonString("Other"))))
    }

    "refuse a fallback case that carries fields" in {
      an[IllegalArgumentException] should be thrownBy decode[Tolerant.Misconfigured](BsonDocument("_type", BsonString("Other")))
    }
  }
