package mongo4s.operations

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

/** One end of a `$setWindowFields` window. */
enum WindowBound:
  /** As far as the partition goes in that direction. */
  case Unbounded

  /** The document being computed. */
  case Current

  /** An offset from the current document — in positions for a `documents` window, in values for a `range` one. */
  case At(offset: Int)

  def toBson: BsonValue = this match
    case Unbounded  => BsonString("unbounded")
    case Current    => BsonString("current")
    case At(offset) => BsonInt32(offset)

/** The span an accumulator sees for each document. Without one the accumulator covers the whole partition. */
final class Window private (val kind: String, val lower: WindowBound, val upper: WindowBound, val unit: Option[DateUnit]):
  def toBson: BsonDocument =
    val document = BsonDocument(kind, BsonArray(List(lower.toBson, upper.toBson).asJava))

    unit.foreach(value => document.append("unit", BsonString(value.wireName)))

    document

object Window:
  /** Counted in documents, so a bound of `At(-2)` means two rows back whatever their values are. */
  def documents(lower: WindowBound, upper: WindowBound): Window = new Window("documents", lower, upper, None)

  /** Counted in the sort field's own values, so rows with equal values fall in the same window. */
  def range(lower: WindowBound, upper: WindowBound): Window = new Window("range", lower, upper, None)

  /** A `range` window over a date field, measured in a unit of time. */
  def rangeOver(lower: WindowBound, upper: WindowBound, unit: DateUnit): Window =
    new Window("range", lower, upper, Some(unit))

/** An accumulator, and the window it is computed over. */
final class WindowOutput[E] private (val accumulator: Accumulator[E], val window: Option[Window]):
  def over(value: Window): WindowOutput[E] = new WindowOutput(accumulator, Some(value))

object WindowOutput:
  def apply[E](accumulator: Accumulator[E]): WindowOutput[E] = new WindowOutput(accumulator, None)
