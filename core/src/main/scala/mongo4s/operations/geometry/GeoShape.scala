package mongo4s.operations.geometry

import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonValue}

import scala.jdk.CollectionConverters.given

enum GeoShape:
  case Within(geometry: Geometry)
  case CenterSphere(centre: Geometry.Point, radiusInRadians: Double)
  case Centre(centre: Geometry.Point, radius: Double)
  case Box(bottomLeft: Geometry.Point, topRight: Geometry.Point)

  def toBson: BsonDocument = this match
    case Within(geometry)             => BsonDocument(Geometry.key, geometry.toBson)
    case CenterSphere(centre, radius) => BsonDocument("$centerSphere", GeoShape.circle(centre, radius))
    case Centre(centre, radius)       => BsonDocument("$center", GeoShape.circle(centre, radius))
    case Box(bottomLeft, topRight)    =>
      BsonDocument(
        "$box",
        BsonArray(List(GeoShape.corner(bottomLeft), GeoShape.corner(topRight)).asJava),
      )

object GeoShape:
  private def corner(point: Geometry.Point): BsonValue =
    BsonArray(List[BsonValue](BsonDouble(point.longitude), BsonDouble(point.latitude)).asJava)

  private def circle(centre: Geometry.Point, radius: Double): BsonValue =
    BsonArray(List[BsonValue](corner(centre), BsonDouble(radius)).asJava)
