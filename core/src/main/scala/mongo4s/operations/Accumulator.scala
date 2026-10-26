package mongo4s.operations

import scala.annotation.unused

import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath, NumericOf, NumericValue}

import scala.jdk.CollectionConverters.given

enum Accumulator[E](val key: String):
  case Sum(expression: Accumulator.Expression[E])      extends Accumulator[E]("$sum")
  case Avg(expression: Accumulator.Expression[E])      extends Accumulator[E]("$avg")
  case Min(expression: Accumulator.Expression[E])      extends Accumulator[E]("$min")
  case Max(expression: Accumulator.Expression[E])      extends Accumulator[E]("$max")
  case First(expression: Accumulator.Expression[E])    extends Accumulator[E]("$first")
  case Last(expression: Accumulator.Expression[E])     extends Accumulator[E]("$last")
  case Push(expression: Accumulator.Expression[E])     extends Accumulator[E]("$push")
  case AddToSet(expression: Accumulator.Expression[E]) extends Accumulator[E]("$addToSet")
  case Count()                                         extends Accumulator[E]("$sum")
  case Raw(document: BsonDocument)                     extends Accumulator[E]("")

  case FirstN(expression: Accumulator.Expression[E], n: Int)               extends Accumulator[E]("$firstN")
  case LastN(expression: Accumulator.Expression[E], n: Int)                extends Accumulator[E]("$lastN")
  case MaxN(expression: Accumulator.Expression[E], n: Int)                 extends Accumulator[E]("$maxN")
  case MinN(expression: Accumulator.Expression[E], n: Int)                 extends Accumulator[E]("$minN")
  case Top(sortBy: Sort[E], output: Accumulator.Expression[E])             extends Accumulator[E]("$top")
  case Bottom(sortBy: Sort[E], output: Accumulator.Expression[E])          extends Accumulator[E]("$bottom")
  case TopN(sortBy: Sort[E], output: Accumulator.Expression[E], n: Int)    extends Accumulator[E]("$topN")
  case BottomN(sortBy: Sort[E], output: Accumulator.Expression[E], n: Int) extends Accumulator[E]("$bottomN")
  case Median(expression: Accumulator.Expression[E])                       extends Accumulator[E]("$median")
  case Percentile(expression: Accumulator.Expression[E], p: List[Double])  extends Accumulator[E]("$percentile")
  case StdDevPop(expression: Accumulator.Expression[E])                    extends Accumulator[E]("$stdDevPop")
  case StdDevSamp(expression: Accumulator.Expression[E])                   extends Accumulator[E]("$stdDevSamp")
  case MergeObjects(expression: Accumulator.Expression[E])                 extends Accumulator[E]("$mergeObjects")

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Raw(document) => document
    case operator      => BsonDocument(key, operator.payload(naming))

  private def payload(naming: FieldNaming): BsonValue = this match
    case Sum(expression)            => expression.toBson(naming)
    case Avg(expression)            => expression.toBson(naming)
    case Min(expression)            => expression.toBson(naming)
    case Max(expression)            => expression.toBson(naming)
    case First(expression)          => expression.toBson(naming)
    case Last(expression)           => expression.toBson(naming)
    case Push(expression)           => expression.toBson(naming)
    case AddToSet(expression)       => expression.toBson(naming)
    case Count()                    => BsonInt32(1)
    case Raw(document)              => document
    case FirstN(expression, n)      => Accumulator.limited(expression, n, naming)
    case LastN(expression, n)       => Accumulator.limited(expression, n, naming)
    case MaxN(expression, n)        => Accumulator.limited(expression, n, naming)
    case MinN(expression, n)        => Accumulator.limited(expression, n, naming)
    case Top(sortBy, output)        => Accumulator.ranked(sortBy, output, None, naming)
    case Bottom(sortBy, output)     => Accumulator.ranked(sortBy, output, None, naming)
    case TopN(sortBy, output, n)    => Accumulator.ranked(sortBy, output, Some(n), naming)
    case BottomN(sortBy, output, n) => Accumulator.ranked(sortBy, output, Some(n), naming)
    case Median(expression)         => Accumulator.approximate(expression, naming)
    case Percentile(expression, p)  =>
      Accumulator.approximate(expression, naming).append("p", BsonArray(p.map(value => BsonDouble(value): BsonValue).asJava))
    case StdDevPop(expression)      => expression.toBson(naming)
    case StdDevSamp(expression)     => expression.toBson(naming)
    case MergeObjects(expression)   => expression.toBson(naming)
  end payload

object Accumulator:

  enum Expression[E]:
    case FieldRef(path: FieldPath)
    case Literal(value: BsonValue)
    case Evaluated(expression: Expr[E, ?])

    def toBson(naming: FieldNaming): BsonValue = this match
      case FieldRef(path)        => BsonString("$" + path.render(naming))
      case Literal(value)        => value
      case Evaluated(expression) => expression.toBson(naming)

  def of[E, A](field: Field[E, A]): Expression[E] = Expression.FieldRef(field.path)

  def sum[E, A](field: Field[E, A]): Accumulator[E]      = Sum(of(field))
  def avg[E, A](field: Field[E, A]): Accumulator[E]      = Avg(of(field))
  def min[E, A](field: Field[E, A]): Accumulator[E]      = Min(of(field))
  def max[E, A](field: Field[E, A]): Accumulator[E]      = Max(of(field))
  def first[E, A](field: Field[E, A]): Accumulator[E]    = First(of(field))
  def last[E, A](field: Field[E, A]): Accumulator[E]     = Last(of(field))
  def push[E, A](field: Field[E, A]): Accumulator[E]     = Push(of(field))
  def addToSet[E, A](field: Field[E, A]): Accumulator[E] = AddToSet(of(field))
  def count[E]: Accumulator[E]                           = Count()

  def sum[E, A](expression: Expr[E, A])(using @unused numeric: NumericValue[A]): Accumulator[E] = Sum(Expression.Evaluated(expression))
  def avg[E, A](expression: Expr[E, A])(using @unused numeric: NumericValue[A]): Accumulator[E] = Avg(Expression.Evaluated(expression))
  def min[E, A](expression: Expr[E, A]): Accumulator[E]                                         = Min(Expression.Evaluated(expression))
  def max[E, A](expression: Expr[E, A]): Accumulator[E]                                         = Max(Expression.Evaluated(expression))
  def first[E, A](expression: Expr[E, A]): Accumulator[E]                                       = First(Expression.Evaluated(expression))
  def last[E, A](expression: Expr[E, A]): Accumulator[E]                                        = Last(Expression.Evaluated(expression))
  def push[E, A](expression: Expr[E, A]): Accumulator[E]                                        = Push(Expression.Evaluated(expression))
  def raw[E](document: BsonDocument): Accumulator[E]                                            = Raw(document)

  def firstN[E, A](field: Field[E, A], n: Int): Accumulator[E] = FirstN(of(field), Bounds.atLeastOne(n, "$firstN n", "which MongoDB refuses"))
  def lastN[E, A](field: Field[E, A], n: Int): Accumulator[E]  = LastN(of(field), Bounds.atLeastOne(n, "$lastN n", "which MongoDB refuses"))
  def maxN[E, A](field: Field[E, A], n: Int): Accumulator[E]   = MaxN(of(field), Bounds.atLeastOne(n, "$maxN n", "which MongoDB refuses"))
  def minN[E, A](field: Field[E, A], n: Int): Accumulator[E]   = MinN(of(field), Bounds.atLeastOne(n, "$minN n", "which MongoDB refuses"))

  def top[E, A](sortBy: Sort[E], output: Field[E, A]): Accumulator[E]    = Top(sorted(sortBy, "$top"), of(output))
  def bottom[E, A](sortBy: Sort[E], output: Field[E, A]): Accumulator[E] = Bottom(sorted(sortBy, "$bottom"), of(output))

  def topN[E, A](sortBy: Sort[E], output: Field[E, A], n: Int): Accumulator[E] =
    TopN(sorted(sortBy, "$topN"), of(output), Bounds.atLeastOne(n, "$topN n", "which MongoDB refuses"))

  def bottomN[E, A](sortBy: Sort[E], output: Field[E, A], n: Int): Accumulator[E] =
    BottomN(sorted(sortBy, "$bottomN"), of(output), Bounds.atLeastOne(n, "$bottomN n", "which MongoDB refuses"))

  def median[E, A](field: Field[E, A])(using @unused numeric: NumericOf[A, ?]): Accumulator[E] = Median(of(field))

  def percentile[E, A](field: Field[E, A], p: Double, more: Double*)(using @unused numeric: NumericOf[A, ?]): Accumulator[E] =
    val all = p :: more.toList
    all.foreach(value => require(value >= 0 && value <= 1, s"$$percentile takes values between 0 and 1, got $value"))
    Percentile(of(field), all)
  end percentile

  def stdDevPop[E, A](field: Field[E, A])(using @unused numeric: NumericOf[A, ?]): Accumulator[E]  = StdDevPop(of(field))
  def stdDevSamp[E, A](field: Field[E, A])(using @unused numeric: NumericOf[A, ?]): Accumulator[E] = StdDevSamp(of(field))

  def mergeObjects[E, A](field: Field[E, A]): Accumulator[E] = MergeObjects(of(field))

  private def sorted[E](sortBy: Sort[E], name: String): Sort[E] =
    require(!sortBy.isEmpty, s"$name ranks by its sortBy, so it needs at least one field to sort on")
    sortBy

  private def limited[E](expression: Expression[E], n: Int, naming: FieldNaming): BsonDocument =
    BsonDocument("input", expression.toBson(naming)).append("n", BsonInt32(n))

  private def ranked[E](sortBy: Sort[E], output: Expression[E], n: Option[Int], naming: FieldNaming): BsonDocument =
    val document = BsonDocument("sortBy", sortBy.toBson(naming)).append("output", output.toBson(naming))
    n.foreach(value => document.append("n", BsonInt32(value)))
    document

  private def approximate[E](expression: Expression[E], naming: FieldNaming): BsonDocument =
    BsonDocument("input", expression.toBson(naming)).append("method", BsonString("approximate"))
