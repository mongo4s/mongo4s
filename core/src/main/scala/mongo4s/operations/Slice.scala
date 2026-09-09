package mongo4s.operations

import org.bson.{BsonArray, BsonInt32, BsonValue}

import scala.jdk.CollectionConverters.given

final case class Slice(count: Int, skip: Option[Int]):
  require(
    skip.isEmpty || count > 0,
    s"A slice that skips must take a positive count, got $count",
  )

  def toBson: BsonValue = skip match
    case None       => BsonInt32(count)
    case Some(from) => BsonArray(List[BsonValue](BsonInt32(from), BsonInt32(count)).asJava)
