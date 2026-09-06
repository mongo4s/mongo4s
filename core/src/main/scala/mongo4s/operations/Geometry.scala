package mongo4s.operations

import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

enum Geometry:
  case Point(longitude: Double, latitude: Double)
  case LineString(points: List[Geometry.Point])
  case Polygon(exterior: List[Geometry.Point], holes: List[List[Geometry.Point]])
  case MultiPoint(points: List[Geometry.Point])
  case MultiLineString(lines: List[List[Geometry.Point]])
  case MultiPolygon(polygons: List[Geometry.Polygon])

  def typeName: String = this match
    case _: Point           => "Point"
    case _: LineString      => "LineString"
    case _: Polygon         => "Polygon"
    case _: MultiPoint      => "MultiPoint"
    case _: MultiLineString => "MultiLineString"
    case _: MultiPolygon    => "MultiPolygon"

  def coordinates: BsonValue = this match
    case point: Point           => Geometry.pair(point)
    case LineString(points)     => Geometry.line(points)
    case polygon: Polygon       => Geometry.rings(polygon)
    case MultiPoint(points)     => Geometry.line(points)
    case MultiLineString(lines) => BsonArray(lines.map(Geometry.line).asJava)
    case MultiPolygon(polygons) => BsonArray(polygons.map(Geometry.rings).asJava)

  def toBson: BsonDocument = BsonDocument("type", BsonString(typeName)).append("coordinates", coordinates)

object Geometry:
  object Polygon:
    def apply(exterior: List[Point]): Polygon = Polygon(exterior, Nil)

  private def pair(point: Point): BsonValue =
    BsonArray(List[BsonValue](BsonDouble(point.longitude), BsonDouble(point.latitude)).asJava)

  private def line(points: List[Point]): BsonValue = BsonArray(points.map(pair).asJava)

  private def rings(polygon: Polygon): BsonValue =
    val all = polygon.exterior :: polygon.holes

    all.foreach { ring =>
      require(
        ring.sizeIs >= 4 && ring.headOption == ring.lastOption,
        "a GeoJSON polygon ring must be closed — repeat the first point as the last one, giving at least four in all",
      )
    }

    BsonArray(all.map(line).asJava)

enum GeoShape:
  case Within(geometry: Geometry)
  case CenterSphere(centre: Geometry.Point, radiusInRadians: Double)
  case Centre(centre: Geometry.Point, radius: Double)
  case Box(bottomLeft: Geometry.Point, topRight: Geometry.Point)

  def toBson: BsonDocument = this match
    case Within(geometry)             => BsonDocument("$geometry", geometry.toBson)
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
