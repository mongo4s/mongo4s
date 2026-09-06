package mongo4s.operations

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

enum DateUnit(val wireName: String):
  case Millisecond extends DateUnit("millisecond")
  case Second      extends DateUnit("second")
  case Minute      extends DateUnit("minute")
  case Hour        extends DateUnit("hour")
  case Day         extends DateUnit("day")
  case Week        extends DateUnit("week")
  case Month       extends DateUnit("month")
  case Quarter     extends DateUnit("quarter")
  case Year        extends DateUnit("year")

enum DensifyBounds:
  case Full
  case Partition
  case Between(lower: BsonValue, upper: BsonValue)

  def toBson: BsonValue = this match
    case Full            => BsonString("full")
    case Partition       => BsonString("partition")
    case Between(lo, hi) => BsonArray(List(lo, hi).asJava)

final class DensifyRange private (val step: BsonValue, val unit: Option[DateUnit], val bounds: DensifyBounds):
  def within(value: DensifyBounds): DensifyRange = new DensifyRange(step, unit, value)

  def toBson: BsonDocument =
    val document = BsonDocument("step", step).append("bounds", bounds.toBson)

    unit.foreach(value => document.append("unit", BsonString(value.wireName)))

    document

object DensifyRange:
  def by(step: Int): DensifyRange = new DensifyRange(BsonInt32(step), None, DensifyBounds.Full)

  def every(step: Int, unit: DateUnit): DensifyRange = new DensifyRange(BsonInt32(step), Some(unit), DensifyBounds.Full)
