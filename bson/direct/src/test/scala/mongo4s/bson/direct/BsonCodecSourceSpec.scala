package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonInt64, BsonString, BsonValue}

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError, BsonTypeName}

object BsonCodecSourceSpec:

  opaque type PlayerId = Long

  object PlayerId:
    def apply(value: Long): PlayerId = value

    given ScalarWireCodec[PlayerId] = ScalarWireCodec[Long].imap(PlayerId.apply)(identity)

  opaque type LegacyId = String

  object LegacyId:
    def apply(value: String): LegacyId = value

    given ScalarWireCodec[LegacyId] = ScalarWireCodec[String].imap(LegacyId.apply)(identity)
    given BsonEncoder[LegacyId]     = id => BsonString(s"legacy:$id")

  final case class Address(city: String, zipCode: String) derives WireCodec

  object Imported:
    opaque type Code = String

    def code(value: String): Code = value

    object Codecs:
      given ScalarWireCodec[Code] = ScalarWireCodec[String].imap(code)(identity)

final class BsonCodecSourceSpec extends AnyWordSpec, Matchers:
  import BsonCodecSourceSpec.*

  "a ScalarWireCodec" should {

    "be the BsonEncoder and BsonDecoder of its type, with no second given" in {
      BsonEncoder[PlayerId].encode(PlayerId(7L)) shouldBe BsonInt64(7L)
      BsonDecoder[PlayerId].decode(BsonInt64(7L)) shouldBe Right(PlayerId(7L))
    }

    "decode with the same leniency and the same errors as on the wire" in {
      BsonDecoder[PlayerId].decode(BsonInt32(7)) shouldBe Right(PlayerId(7L))
      BsonDecoder[PlayerId].decode(BsonString("seven")) shouldBe Left(BsonError.TypeMismatch(BsonTypeName.Long, BsonTypeName.String))
    }

    "reach through Option and collections the way any BsonEncoder does" in {
      BsonEncoder[Option[PlayerId]].encode(Some(PlayerId(1L))) shouldBe BsonInt64(1L)
      BsonEncoder[List[PlayerId]].encode(List(PlayerId(1L), PlayerId(2L))) shouldBe BsonArray(java.util.List.of(BsonInt64(1L), BsonInt64(2L)))
      BsonDecoder[List[PlayerId]].decode(BsonArray(java.util.List.of(BsonInt64(3L)))) shouldBe Right(List(PlayerId(3L)))
    }

    "give way to a BsonEncoder written for the type by hand" in {
      BsonEncoder[LegacyId].encode(LegacyId("a")) shouldBe BsonString("legacy:a")
      BsonDecoder[LegacyId].decode(BsonString("a")) shouldBe Right(LegacyId("a"))
    }

    "be found where it is imported, not only in the companion" in {
      import Imported.Codecs.given

      BsonEncoder[Imported.Code].encode(Imported.code("x")) shouldBe BsonString("x")
    }

    "not displace the encoder the library already has for a type, so an Instant stored another way needs its encoder too" in {
      given ScalarWireCodec[java.time.Instant] = ScalarWireCodec.instantAsEpochSeconds

      val at = java.time.Instant.ofEpochSecond(10L)

      BsonEncoder[java.time.Instant].encode(at) shouldBe org.bson.BsonDateTime(10000L)
      ScalarWireCodec[java.time.Instant].toBsonEncoder.encode(at) shouldBe BsonInt64(10L)
    }

    "still be the WireCodec of its type, rather than the BsonValue fallback built from the bridge" in {
      (WireCodec[PlayerId] eq summon[ScalarWireCodec[PlayerId]]) shouldBe true
    }
  }

  "WireCodec#toBsonCodec" should {

    "turn a document-shaped codec into a BsonCodec writing the same document" in {
      val codec   = WireCodec[Address].toBsonCodec
      val encoded = codec.encode(Address("Paris", "75001"))

      encoded shouldBe BsonDocument("city", BsonString("Paris")).append("zipCode", BsonString("75001"))
      codec.decode(encoded) shouldBe Right(Address("Paris", "75001"))
    }

    "report a value it cannot read as a BsonError, with its path" in {
      val broken: BsonValue = BsonDocument("city", BsonInt32(1)).append("zipCode", BsonString("75001"))

      WireCodec[Address].toBsonCodec.decode(broken) shouldBe Left(BsonError.Nested("city", BsonError.TypeMismatch(BsonTypeName.String, BsonTypeName.Int)))
    }

    "bridge any shape, an array of documents included" in {
      val codec = WireCodec[List[Address]].toBsonCodec

      codec.decode(codec.encode(List(Address("Rome", "00100")))) shouldBe Right(List(Address("Rome", "00100")))
    }
  }
