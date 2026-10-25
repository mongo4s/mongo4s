package mongo4s.operations

import scala.annotation.unused

import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonInt32, BsonString, BsonValue}

import mongo4s.{Field, NumericOf}
import mongo4s.bson.{BsonEncoder, FieldNaming}

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

sealed trait WindowField[E]:
  def toBson(naming: FieldNaming): BsonDocument

final class WindowOutput[E] private (val accumulator: Accumulator[E], val window: Option[Window]) extends WindowField[E]:
  def over(value: Window): WindowOutput[E] = new WindowOutput(accumulator, Some(value))

  def toBson(naming: FieldNaming): BsonDocument =
    val field = accumulator.toBson(naming)
    window.fold(field)(value => field.clone().append("window", value.toBson))

object WindowOutput:
  def apply[E](accumulator: Accumulator[E]): WindowOutput[E] = new WindowOutput(accumulator, None)

enum WindowFunction[E](val key: String) extends WindowField[E]:
  case Rank()                                                                                    extends WindowFunction[E]("$rank")
  case DenseRank()                                                                               extends WindowFunction[E]("$denseRank")
  case DocumentNumber()                                                                          extends WindowFunction[E]("$documentNumber")
  case Shift(output: Accumulator.Expression[E], by: Int, default: Option[BsonValue])             extends WindowFunction[E]("$shift")
  case Locf(expression: Accumulator.Expression[E])                                               extends WindowFunction[E]("$locf")
  case LinearFill(expression: Accumulator.Expression[E])                                         extends WindowFunction[E]("$linearFill")
  case Derivative(expression: Accumulator.Expression[E], window: Window, unit: Option[DateUnit]) extends WindowFunction[E]("$derivative")
  case Integral(expression: Accumulator.Expression[E], window: Window, unit: Option[DateUnit])   extends WindowFunction[E]("$integral")
  case ExpMovingAvg(expression: Accumulator.Expression[E], smoothing: Either[Int, Double])       extends WindowFunction[E]("$expMovingAvg")

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Rank() | DenseRank() | DocumentNumber() => BsonDocument(key, BsonDocument())

    case Shift(output, by, default) =>
      val shift = BsonDocument("output", output.toBson(naming)).append("by", BsonInt32(by))
      default.foreach(value => shift.append("default", value))
      BsonDocument(key, shift)

    case Locf(expression)       => BsonDocument(key, expression.toBson(naming))
    case LinearFill(expression) => BsonDocument(key, expression.toBson(naming))

    case Derivative(expression, window, unit) => WindowFunction.overWindow(key, expression, window, unit, naming)
    case Integral(expression, window, unit)   => WindowFunction.overWindow(key, expression, window, unit, naming)

    case ExpMovingAvg(expression, smoothing) =>
      val average = BsonDocument("input", expression.toBson(naming))
      smoothing.fold(n => average.append("N", BsonInt32(n)), alpha => average.append("alpha", BsonDouble(alpha)))
      BsonDocument(key, average)
  end toBson

object WindowFunction:
  def rank[E]: WindowFunction[E]           = Rank()
  def denseRank[E]: WindowFunction[E]      = DenseRank()
  def documentNumber[E]: WindowFunction[E] = DocumentNumber()

  def shift[E, A](field: Field[E, A], by: Int, default: Option[A] = None)(using encoder: BsonEncoder[A]): WindowFunction[E] =
    Shift(Accumulator.of(field), by, default.map(encoder.encode))

  def locf[E, A](field: Field[E, A]): WindowFunction[E] = Locf(Accumulator.of(field))

  def linearFill[E, A](field: Field[E, A])(using @unused numeric: NumericOf[A, ?]): WindowFunction[E] = LinearFill(Accumulator.of(field))

  def derivative[E, A](field: Field[E, A], window: Window, unit: Option[DateUnit] = None)(using @unused numeric: NumericOf[A, ?]): WindowFunction[E] =
    Derivative(Accumulator.of(field), window, unit)

  def integral[E, A](field: Field[E, A], window: Window, unit: Option[DateUnit] = None)(using @unused numeric: NumericOf[A, ?]): WindowFunction[E] =
    Integral(Accumulator.of(field), window, unit)

  def expMovingAvg[E, A](field: Field[E, A], n: Int)(using @unused numeric: NumericOf[A, ?]): WindowFunction[E] =
    ExpMovingAvg(Accumulator.of(field), Left(Bounds.atLeastOne(n, "$expMovingAvg N", "which MongoDB refuses")))

  def expMovingAvgAlpha[E, A](field: Field[E, A], alpha: Double)(using @unused numeric: NumericOf[A, ?]): WindowFunction[E] =
    require(alpha > 0 && alpha < 1, s"$$expMovingAvg alpha has to lie strictly between 0 and 1, got $alpha")
    ExpMovingAvg(Accumulator.of(field), Right(alpha))
  end expMovingAvgAlpha

  private def overWindow[E](
      key: String,
      expression: Accumulator.Expression[E],
      window: Window,
      unit: Option[DateUnit],
      naming: FieldNaming,
  ): BsonDocument =
    val operator = BsonDocument("input", expression.toBson(naming))
    unit.foreach(value => operator.append("unit", BsonString(value.wireName)))
    BsonDocument(key, operator).append("window", window.toBson)
  end overWindow
