package mongo4s.operations

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

enum WindowBound:
  case Unbounded
  case Current
  case At(offset: Int)

  def toBson: BsonValue = this match
    case Unbounded  => BsonString("unbounded")
    case Current    => BsonString("current")
    case At(offset) => BsonInt32(offset)

final class Window private (
    val kind: String,
    val lower: WindowBound,
    val upper: WindowBound,
    val unit: Option[DateUnit],
):
  def toBson: BsonDocument =
    val document = BsonDocument(kind, BsonArray(List(lower.toBson, upper.toBson).asJava))

    unit.foreach(value => document.append("unit", BsonString(value.wireName)))

    document

object Window:
  def documents(lower: WindowBound, upper: WindowBound): Window = new Window("documents", lower, upper, None)

  def range(lower: WindowBound, upper: WindowBound): Window = new Window("range", lower, upper, None)

  def rangeOver(lower: WindowBound, upper: WindowBound, unit: DateUnit): Window =
    new Window("range", lower, upper, Some(unit))

final class WindowOutput[E] private (val accumulator: Accumulator[E], val window: Option[Window]):
  def over(value: Window): WindowOutput[E] = new WindowOutput(accumulator, Some(value))

object WindowOutput:
  def apply[E](accumulator: Accumulator[E]): WindowOutput[E] = new WindowOutput(accumulator, None)
