package mongo4s.bson.ziobson

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonInt32, BsonString}

import mongo4s.bson.ziobson.ZioBsonInstances.given

final class ZioBsonInstancesSpec extends AnyWordSpec, Matchers:

  "zio-bson bridge" should {
    "bridge a scalar value through the value encoder, not the document codec" in {
      summon[mongo4s.bson.BsonEncoder[Int]].encode(7) shouldBe BsonInt32(7)
      summon[mongo4s.bson.BsonDecoder[Int]].decode(BsonInt32(7)) shouldBe Right(7)
      summon[mongo4s.bson.BsonEncoder[String]].encode("hi") shouldBe BsonString("hi")
    }
  }
