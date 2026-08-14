package mongo4s.bson.medeia

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonInt32
import medeia.codec.BsonDocumentCodec

import mongo4s.bson.medeia.MedeiaInstances.given

object MedeiaInstancesSpec:
  final case class Point(x: Int, y: Int) derives BsonDocumentCodec

final class MedeiaInstancesSpec extends AnyWordSpec, Matchers:
  import MedeiaInstancesSpec.Point

  "medeia bridge" should {
    "round-trip a document codec" in {
      val codec    = summon[mongo4s.bson.BsonDocumentCodec[Point]]
      val document = codec.encodeDocument(Point(1, 2))
      codec.decodeDocument(document) shouldBe Right(Point(1, 2))
    }

    "bridge a scalar value through the value encoder, not the document codec" in {
      summon[mongo4s.bson.BsonEncoder[Int]].encode(5) shouldBe BsonInt32(5)
      summon[mongo4s.bson.BsonDecoder[Int]].decode(BsonInt32(5)) shouldBe Right(5)
    }
  }
