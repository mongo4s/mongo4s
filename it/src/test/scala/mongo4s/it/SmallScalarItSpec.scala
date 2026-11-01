package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.BsonDocument

import mongo4s.bson.BsonTypeName
import mongo4s.bson.direct.WireCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.{Filter, Update}
import mongo4s.{Field, MongoClient}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object SmallScalarItSpec:
  final case class Sensor(name: String, ratio: Float, floor: Short, flags: Byte, grade: Char, payload: Array[Byte]) derives WireCodec

final class SmallScalarItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import SmallScalarItSpec.Sensor

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val ratio   = Field.of[Sensor](_.ratio)
  private val floor   = Field.of[Sensor](_.floor)
  private val flags   = Field.of[Sensor](_.flags)
  private val grade   = Field.of[Sensor](_.grade)
  private val payload = Field.of[Sensor](_.payload)

  private val sensors = List(
    Sensor("low", 0.25f, 1, 3, 'A', Array[Byte](1, 2)),
    Sensor("high", 2.5f, 7, 120, 'C', Array[Byte](9)),
  )

  "Float, Short, Byte, Char and Array[Byte]" should {

    "be stored as double, int, string and binary, queried on, updated and read back" in {
      val program =
        for
          client    <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database  <- client.getDatabase("small_scalars_it")
          direct    <- database.getDirectCollection[Sensor]("sensors")
          raw       <- database.getCollection[BsonDocument]("sensors")
          _         <- direct.insertMany(sensors)
          typed     <- raw
                         .count(
                           Filter.and(
                             Field.stored[BsonDocument, Any]("ratio").hasType(BsonTypeName.Double),
                             Field.stored[BsonDocument, Any]("floor").hasType(BsonTypeName.Int),
                             Field.stored[BsonDocument, Any]("flags").hasType(BsonTypeName.Int),
                             Field.stored[BsonDocument, Any]("grade").hasType(BsonTypeName.String),
                             Field.stored[BsonDocument, Any]("payload").hasType(BsonTypeName.BinData),
                           )
                         )
          byRatio   <- direct.find(ratio.gt(1f)).all.map(_.map(_.name))
          byFloor   <- direct.find(floor.equalTo(1.toShort)).all.map(_.map(_.name))
          byFlags   <- direct.find(flags.gte(100.toByte)).all.map(_.map(_.name))
          byGrade   <- direct.find(grade.lt('B')).all.map(_.map(_.name))
          byPayload <- direct.find(payload.equalTo(Array[Byte](9))).all.map(_.map(_.name))
          _         <- direct.updateOne(Field.of[Sensor](_.name).equalTo("low"), Update.inc(floor, 2.toShort))
          low       <- direct.find(Field.of[Sensor](_.name).equalTo("low")).first
          _         <- client.close
        yield (typed, byRatio, byFloor, byFlags, byGrade, byPayload, low.map(sensor => (sensor.floor, sensor.ratio, sensor.payload.toList)))

      program
        .timeout(30.seconds)
        .asserting(
          _ shouldBe (2L, List("high"), List("low"), List("high"), List("low"), List("high"), Some((3.toShort, 0.25f, List[Byte](1, 2))))
        )
    }
  }
