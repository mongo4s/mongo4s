package mongo4s.bson

import java.time.Instant

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonInt32, BsonString}

import mongo4s.bson.BsonInstances.given

final class BsonErrorSpec extends AnyWordSpec with Matchers:

  "TypeMismatch" should {
    "describe both sides with mongo type aliases" in {
      BsonDecoder[Instant].decode(BsonString("nope")).left.map(_.message) shouldBe
        Left("Expected date but got string")
    }

    "name objectId the way mongo does" in {
      BsonDecoder[org.bson.types.ObjectId].decode(BsonInt32(1)).left.map(_.message) shouldBe
        Left("Expected objectId but got int")
    }

    "report a document as object" in {
      BsonDecoder[Map[String, String]].decode(BsonString("nope")).left.map(_.message) shouldBe
        Left("Expected object but got string")
    }
  }
