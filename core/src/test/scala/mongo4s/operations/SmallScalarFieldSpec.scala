package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object SmallScalarFieldSpec:
  final case class Sensor(ratio: Float, floor: Short, flags: Byte, grade: Char, payload: Array[Byte])

final class SmallScalarFieldSpec extends AnyWordSpec, Matchers:
  import SmallScalarFieldSpec.Sensor

  private val ratio   = Field.of[Sensor](_.ratio)
  private val floor   = Field.of[Sensor](_.floor)
  private val flags   = Field.of[Sensor](_.flags)
  private val grade   = Field.of[Sensor](_.grade)
  private val payload = Field.of[Sensor](_.payload)

  private def json(filter: Filter[Sensor]): String = filter.toBson(FieldNaming.identity).toJson
  private def json(update: Update[Sensor]): String = update.toBson(FieldNaming.identity).toJson

  "fields of Float, Short, Byte, Char and Array[Byte]" should {

    "filter and compare as the double, int, string and binary they are stored as" in {
      json(ratio.gt(0.5f)) shouldBe """{"ratio": {"$gt": 0.5}}"""
      json(floor.equalTo(3.toShort)) shouldBe """{"floor": 3}"""
      json(flags.lte(7.toByte)) shouldBe """{"flags": {"$lte": 7}}"""
      json(grade.gte('B')) shouldBe """{"grade": {"$gte": "B"}}"""
      json(payload.equalTo(Array[Byte](1, 2))) shouldBe """{"payload": {"$binary": {"base64": "AQI=", "subType": "00"}}}"""
    }

    "take the numeric update operators" in {
      json(Update.inc(floor, 1.toShort)) shouldBe """{"$inc": {"floor": 1}}"""
      json(Update.mul(ratio, 2f)) shouldBe """{"$mul": {"ratio": 2.0}}"""
      json(Update.max(flags, 9.toByte)) shouldBe """{"$max": {"flags": 9}}"""
    }
  }
