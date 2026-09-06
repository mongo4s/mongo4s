package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.operations.{GeoShape, Geometry, Index}
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

final class GeoItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val location = Field.stored[BsonDocument, Geometry.Point]("location")

  private def place(name: String, longitude: Double, latitude: Double): BsonDocument =
    BsonDocument("name", BsonString(name)).append("location", Geometry.Point(longitude, latitude).toBson)

  private val berlin: Geometry.Point = Geometry.Point(13.4050, 52.5200)

  private def seeded(name: String): IO[MongoCollection[IO, S, BsonDocument]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("geo_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.createIndex(Index.geo2dsphere(location))
      _          <- collection.insertMany(List(place("nearby", 13.4230, 52.5200), place("faraway", 13.9000, 52.5200)))
    yield collection

  private def namesOf(documents: List[BsonDocument]): List[String] =
    documents.map(_.getString("name").getValue)

  "$near" should {
    "return only what is inside maxDistance, nearest first" in {
      val program =
        for
          collection <- seeded("near_close")
          found      <- collection.find(location.near(berlin, maxDistance = Some(5000))).all
        yield namesOf(found)

      program.timeout(30.seconds).asserting(_ shouldBe List("nearby"))
    }

    "widen with the distance" in {
      val program =
        for
          collection <- seeded("near_wide")
          found      <- collection.find(location.near(berlin, maxDistance = Some(100000))).all
        yield namesOf(found)

      program.timeout(30.seconds).asserting(_ shouldBe List("nearby", "faraway"))
    }
  }

  "$geoWithin" should {
    "match what a polygon contains" in {
      val box = Geometry.Polygon(
        List(
          Geometry.Point(13.40, 52.50),
          Geometry.Point(13.40, 52.54),
          Geometry.Point(13.45, 52.54),
          Geometry.Point(13.45, 52.50),
          Geometry.Point(13.40, 52.50),
        )
      )

      val program =
        for
          collection <- seeded("within_polygon")
          found      <- collection.find(location.within(GeoShape.Within(box))).all
        yield namesOf(found)

      program.timeout(30.seconds).asserting(_ shouldBe List("nearby"))
    }

    "match a legacy sphere around a centre" in {
      val program =
        for
          collection <- seeded("within_sphere")
          found      <- collection.find(location.within(GeoShape.CenterSphere(berlin, 5.0 / 6378.1))).all
        yield namesOf(found)

      program.timeout(30.seconds).asserting(_ shouldBe List("nearby"))
    }
  }

  "$geoIntersects" should {
    "match a document whose point falls inside the shape" in {
      val around = Geometry.Polygon(
        List(
          Geometry.Point(13.41, 52.51),
          Geometry.Point(13.41, 52.53),
          Geometry.Point(13.44, 52.53),
          Geometry.Point(13.44, 52.51),
          Geometry.Point(13.41, 52.51),
        )
      )

      val program =
        for
          collection <- seeded("intersects")
          found      <- collection.find(location.intersects(around)).all
        yield namesOf(found)

      program.timeout(30.seconds).asserting(_ shouldBe List("nearby"))
    }
  }
