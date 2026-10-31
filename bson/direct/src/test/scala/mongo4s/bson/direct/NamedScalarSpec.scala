package mongo4s.bson.direct

import java.util.UUID
import java.time.{Instant, LocalDate, ZoneId}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.*

import mongo4s.bson.{BsonDecoder, BsonError}

final class NamedScalarSpec extends AnyWordSpec, Matchers:

  private def write[A](codec: WireCodec[A], value: A): BsonValue = codec.toBsonCodec.encode(value)

  private def read[A](codec: WireCodec[A], value: BsonValue): Either[BsonError, A] = codec.toBsonCodec.decode(value)

  private val instant = Instant.parse("2026-10-31T12:34:56Z")
  private val day     = LocalDate.parse("2026-10-31")
  private val uuid    = UUID.fromString("3b241101-e2bb-4255-8caf-4136c566a962")

  "the named Instant codecs" should {

    "store an Instant as epoch milliseconds, epoch seconds or an ISO string, and read it back" in {
      write(ScalarWireCodec.instantAsEpochMillis, instant) shouldBe BsonInt64(instant.toEpochMilli)
      write(ScalarWireCodec.instantAsEpochSeconds, instant) shouldBe BsonInt64(instant.getEpochSecond)
      write(ScalarWireCodec.instantAsIsoString, instant) shouldBe BsonString("2026-10-31T12:34:56Z")

      read(ScalarWireCodec.instantAsEpochSeconds, BsonInt32(instant.getEpochSecond.toInt)) shouldBe Right(instant)
      read(ScalarWireCodec.instantAsIsoString, BsonString("2026-10-31T12:34:56Z")) shouldBe Right(instant)
    }

    "report an ISO string it cannot parse as an invalid value" in {
      read(ScalarWireCodec.instantAsIsoString, BsonString("yesterday")).left.toOption.get shouldBe a[BsonError.InvalidValue]
    }
  }

  "the named LocalDate codecs" should {

    "store a date as an ISO string, or as a BSON date at the start of the day in a zone" in {
      val berlin = ZoneId.of("Europe/Berlin")

      write(ScalarWireCodec.localDateAsString, day) shouldBe BsonString("2026-10-31")
      write(ScalarWireCodec.localDateAsDate(), day) shouldBe BsonDateTime(Instant.parse("2026-10-31T00:00:00Z").toEpochMilli)
      write(ScalarWireCodec.localDateAsDate(berlin), day) shouldBe BsonDateTime(Instant.parse("2026-10-30T23:00:00Z").toEpochMilli)

      read(ScalarWireCodec.localDateAsDate(berlin), BsonDateTime(Instant.parse("2026-10-30T23:00:00Z").toEpochMilli)) shouldBe Right(day)
      read(ScalarWireCodec.localDateAsString, BsonString("2026-10-31")) shouldBe Right(day)
    }
  }

  "a UUID" should {

    "be read from the binary subtype 4 every other driver writes, on both paths" in {
      val binary = BsonBinary(uuid)

      BsonDecoder[UUID].decode(binary) shouldBe Right(uuid)
      read(WireCodec[UUID], binary) shouldBe Right(uuid)
    }

    "refuse a legacy subtype 3, whose byte order it cannot know" in {
      BsonDecoder[UUID].decode(BsonBinary(BsonBinarySubType.UUID_LEGACY, new Array[Byte](16))).left.toOption.get shouldBe a[BsonError.InvalidValue]
    }

    "be written as binary by uuidAsBinary, which still reads the string form" in {
      write(ScalarWireCodec.uuidAsBinary, uuid) shouldBe BsonBinary(uuid)
      read(ScalarWireCodec.uuidAsBinary, BsonString(uuid.toString)) shouldBe Right(uuid)
      read(ScalarWireCodec.uuidAsBinary, BsonBinary(uuid)) shouldBe Right(uuid)
    }
  }

  "ScalarWireCodec.byValue" should {
    enum Provider(val value: String):
      case Stripe extends Provider("stripe")
      case Adyen  extends Provider("adyen")

    val codec = ScalarWireCodec.byValue(Provider.values)(_.value)

    "store each case as its value and read it back" in {
      write(codec, Provider.Adyen) shouldBe BsonString("adyen")
      read(codec, BsonString("stripe")) shouldBe Right(Provider.Stripe)
    }

    "report a value no case is stored as" in {
      read(codec, BsonString("paypal")) shouldBe Left(BsonError.InvalidValue("no case is stored as paypal"))
    }

    "refuse two cases stored as the same value" in {
      an[IllegalArgumentException] should be thrownBy ScalarWireCodec.byValue(List("a", "b"))(_ => 1)
    }
  }

  "WireCodec.decodeOnly" should {
    "read with the decoder it was given, and refuse to write" in {
      val codec = WireCodec.decodeOnly[Int](reader => reader.readInt32() * 2)

      read(codec, BsonInt32(21)) shouldBe Right(42)
      an[UnsupportedOperationException] should be thrownBy write(codec, 1)
    }
  }
