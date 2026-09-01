package mongo4s.bson

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.types.Decimal128
import org.bson.{BsonDecimal128, BsonDouble, BsonInt32, BsonInt64}

import mongo4s.bson.BsonInstances.given

final class NumericDecoderSpec extends AnyWordSpec, Matchers:

  "BsonDecoder[Int]" should {
    "read an in-range value" in {
      BsonDecoder[Int].decode(BsonInt32(42)) shouldBe Right(42)
      BsonDecoder[Int].decode(BsonInt64(42L)) shouldBe Right(42)
    }

    "reject a value beyond Int's range instead of truncating it" in {
      BsonDecoder[Int].decode(BsonInt64(1L << 40)).isLeft shouldBe true
      BsonDecoder[Int].decode(BsonDouble(1e30)).isLeft shouldBe true
    }

    "reject a fractional value instead of rounding it" in {
      BsonDecoder[Int].decode(BsonDouble(3.9)).isLeft shouldBe true
    }
  }

  "BsonDecoder[Long]" should {
    "read an in-range value" in {
      BsonDecoder[Long].decode(BsonInt64(1L << 40)) shouldBe Right(1L << 40)
    }

    "reject a value beyond Long's range" in {
      BsonDecoder[Long].decode(BsonDouble(1e30)).isLeft shouldBe true
    }

    "reject a fractional value" in {
      BsonDecoder[Long].decode(BsonDouble(3.5)).isLeft shouldBe true
    }

    "read a whole Decimal128 exactly rather than routing it through Double" in {
      val value = Decimal128(java.math.BigDecimal("9007199254740993"))

      BsonDecoder[Long].decode(BsonDecimal128(value)) shouldBe Right(9007199254740993L)
    }

    "reject a Double sitting exactly on 2^63, which is one past Long's range" in {
      BsonDecoder[Long].decode(BsonDouble(9.223372036854776e18)).isLeft shouldBe true
    }

    "report a non-finite Decimal128 as an error rather than throwing" in {
      BsonDecoder[Long].decode(BsonDecimal128(Decimal128.NaN)).isLeft shouldBe true
      BsonDecoder[Long].decode(BsonDecimal128(Decimal128.POSITIVE_INFINITY)).isLeft shouldBe true
    }
  }

  "BsonDecoder[BigDecimal]" should {
    "keep integer precision instead of routing through Double" in {
      BsonDecoder[BigDecimal].decode(BsonInt64(9007199254740993L)) shouldBe Right(BigDecimal(9007199254740993L))
    }

    "read a Decimal128 exactly" in {
      val value = BigDecimal("1.2345678901234567890123456789")
      BsonDecoder[BigDecimal].decode(BsonDecimal128(Decimal128(value.bigDecimal))) shouldBe Right(value)
    }

    "report a non-finite Decimal128 as an error rather than throwing" in {
      BsonDecoder[BigDecimal].decode(BsonDecimal128(Decimal128.NaN)).isLeft shouldBe true
      BsonDecoder[BigDecimal].decode(BsonDecimal128(Decimal128.NEGATIVE_INFINITY)).isLeft shouldBe true
    }

    "read a negative zero as plain zero, since BigDecimal has no signed zero" in {
      BsonDecoder[BigDecimal].decode(BsonDecimal128(Decimal128.NEGATIVE_ZERO)) shouldBe Right(BigDecimal(0))
    }
  }
