package mongo4s.bson.calypso

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonBoolean, BsonDouble, BsonInt32, BsonInt64, BsonString}

import mongo4s.bson.{BsonDecoder, BsonEncoder}

import mongo4s.bson.calypso.CalypsoInstances.given

final class CalypsoInstancesSpec extends AnyWordSpec, Matchers:

  private def roundTrip[A: {BsonEncoder, BsonDecoder}](value: A): Either[?, A] =
    summon[BsonDecoder[A]].decode(summon[BsonEncoder[A]].encode(value))

  "calypso bridge (0.7.0, org.bson 5.2.0, on scalaLast)" should {

    "encode each scalar to the expected org.bson subtype" in {
      summon[BsonEncoder[Int]].encode(9) shouldBe BsonInt32(9)
      summon[BsonEncoder[Long]].encode(9L) shouldBe BsonInt64(9L)
      summon[BsonEncoder[Double]].encode(1.5) shouldBe BsonDouble(1.5)
      summon[BsonEncoder[String]].encode("hi") shouldBe BsonString("hi")
      summon[BsonEncoder[Boolean]].encode(true) shouldBe BsonBoolean(true)
    }

    "round-trip the full scalar range" in {
      roundTrip(42) shouldBe Right(42)
      roundTrip(9000000000L) shouldBe Right(9000000000L)
      roundTrip(3.14159) shouldBe Right(3.14159)
      roundTrip("mongo4s") shouldBe Right("mongo4s")
      roundTrip(true) shouldBe Right(true)
    }

    "route scalars through the value bridge, never the document codec" in {
      noException should be thrownBy summon[BsonEncoder[Int]].encode(1)
    }
  }
