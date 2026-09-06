package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

object GeoSpec:
  final case class Place(name: String, lastSeen: Geometry.Point, serviceArea: Geometry.Polygon)

final class GeoSpec extends AnyWordSpec, Matchers:
  import GeoSpec.Place

  private val lastSeen = Field.of[Place, Geometry.Point](_.lastSeen)
  private val area     = Field.of[Place, Geometry.Polygon](_.serviceArea)

  private val square = Geometry.Polygon(
    List(
      Geometry.Point(0, 0),
      Geometry.Point(0, 1),
      Geometry.Point(1, 1),
      Geometry.Point(1, 0),
      Geometry.Point(0, 0),
    )
  )

  "geometry" should {
    "render a point longitude first" in {
      Geometry.Point(13.4, 52.5).toBson.toJson shouldBe """{"type": "Point", "coordinates": [13.4, 52.5]}"""
    }

    "wrap a polygon's rings in an outer array" in {
      square.toBson.toJson shouldBe
        """{"type": "Polygon", "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]]}"""
    }

    "refuse a ring that does not close" in {
      val open = Geometry.Polygon(List(Geometry.Point(0, 0), Geometry.Point(0, 1), Geometry.Point(1, 1)))

      an[IllegalArgumentException] should be thrownBy open.toBson
    }
  }

  "geospatial filters" should {
    "render $near with its distance bounds, through the naming policy" in {
      val filter = lastSeen.near(Geometry.Point(13.4, 52.5), maxDistance = Some(1500), minDistance = Some(10))

      filter.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"last_seen": {"$near": {"$geometry": {"type": "Point", "coordinates": [13.4, 52.5]}, """ +
        """"$minDistance": 10.0, "$maxDistance": 1500.0}}}"""
    }

    "render $nearSphere when the sphere is asked for" in {
      val filter = lastSeen.nearSphere(Geometry.Point(0, 0))

      filter.toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"last_seen": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": [0.0, 0.0]}}}}"""
    }

    "render $geoWithin over a GeoJSON shape" in {
      lastSeen.within(GeoShape.Within(square)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"last_seen": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": """ +
        """[[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]]}}}}"""
    }

    "render the legacy circle and box shapes" in {
      lastSeen.within(GeoShape.CenterSphere(Geometry.Point(1, 2), 0.05)).toBson(FieldNaming.identity).toJson shouldBe
        """{"lastSeen": {"$geoWithin": {"$centerSphere": [[1.0, 2.0], 0.05]}}}"""

      lastSeen.within(GeoShape.Box(Geometry.Point(0, 0), Geometry.Point(1, 1))).toBson(FieldNaming.identity).toJson shouldBe
        """{"lastSeen": {"$geoWithin": {"$box": [[0.0, 0.0], [1.0, 1.0]]}}}"""
    }

    "render $geoIntersects" in {
      area.intersects(Geometry.Point(0.5, 0.5)).toBson(FieldNaming.snakeCase).toJson shouldBe
        """{"service_area": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [0.5, 0.5]}}}}"""
    }
  }
