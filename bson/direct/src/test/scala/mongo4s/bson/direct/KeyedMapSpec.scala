package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonValue}

import mongo4s.bson.BsonError

object KeyedMapSpec:
  enum Provider(val value: String):
    case Stripe extends Provider("stripe")
    case Adyen  extends Provider("adyen")

  object Provider:
    given ScalarWireCodec[Provider] = ScalarWireCodec.byValue(Provider.values)(_.value)

  final case class Totals(byProvider: Map[Provider, Int], byYear: Map[Int, Int], byName: Map[String, Int]) derives WireCodec

final class KeyedMapSpec extends AnyWordSpec, Matchers:
  import KeyedMapSpec.*

  private val codec = WireCodec[Totals].toBsonCodec

  "a Map with keys other than String" should {

    "be a document keyed by each key's string form, read back into the key type" in {
      val totals  = Totals(Map(Provider.Stripe -> 1, Provider.Adyen -> 2), Map(2026 -> 3), Map("a" -> 4))
      val encoded = codec.encode(totals).asDocument

      encoded.getDocument("byProvider") shouldBe BsonDocument("stripe", BsonInt32(1)).append("adyen", BsonInt32(2))
      encoded.getDocument("byYear") shouldBe BsonDocument("2026", BsonInt32(3))
      codec.decode(encoded) shouldBe Right(totals)
    }

    "name a stored key no case matches, and where" in {
      val stored: BsonValue = BsonDocument("byProvider", BsonDocument("paypal", BsonInt32(1)))
        .append("byYear", BsonDocument())
        .append("byName", BsonDocument())

      codec.decode(stored) shouldBe Left(
        BsonError.Nested("byProvider", BsonError.Nested("paypal", BsonError.InvalidValue("no case is stored as paypal")))
      )
    }

    "leave a Map keyed by String to its own codec" in {
      WireCodec[Map[String, Int]].getClass.getName should include("mapWireCodec")
    }

    "need a key codec that writes a string" in {
      given ScalarWireCodec[Provider] = ScalarWireCodec[Int].imap(Provider.fromOrdinal)(_.ordinal)

      an[IllegalArgumentException] should be thrownBy WireCodec[Map[Provider, Int]].toBsonCodec.encode(Map(Provider.Adyen -> 1))
    }
  }
