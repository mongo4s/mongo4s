package mongo4s.operations

import java.time.Instant

import scala.annotation.{targetName, unused}

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.{ElementOf, Field, FieldPath, NumericValue}
import mongo4s.bson.{BsonEncoder, FieldNaming}

import scala.jdk.CollectionConverters.given

enum Expr[-E, +A]:
  case FieldRef(path: FieldPath)                             extends Expr[Any, Nothing]
  case Variable(name: String, path: FieldPath)               extends Expr[Any, Nothing]
  case Literal(value: BsonValue)                             extends Expr[Any, Nothing]
  case Operator(name: String, args: List[Expr[?, ?]])        extends Expr[Any, Nothing]
  case Unary(name: String, arg: Expr[?, ?])                  extends Expr[Any, Nothing]
  case Named(name: String, args: List[(String, Expr[?, ?])]) extends Expr[Any, Nothing]
  case GetField(path: FieldPath, input: Expr[?, ?])          extends Expr[Any, Nothing]
  case Document(fields: List[(FieldPath, Expr[?, ?])])       extends Expr[Any, Nothing]
  case ArrayOf(elements: List[Expr[?, ?]])                   extends Expr[Any, Nothing]
  case Raw(value: BsonValue)                                 extends Expr[Any, Nothing]

  def toBson(naming: FieldNaming): BsonValue =
    this match
      case FieldRef(path)        => BsonString("$" + path.render(naming))
      case Variable(name, path)  => BsonString("$$" + name + (if path.isEmpty then "" else "." + path.render(naming)))
      case Literal(value)        => if Expr.ambiguous(value) then BsonDocument("$literal", value) else value
      case Operator(name, args)  => BsonDocument(name, BsonArray(args.map(_.toBson(naming)).asJava))
      case Unary(name, arg)      => BsonDocument(name, arg.toBson(naming))
      case Named(name, args)     => BsonDocument(name, args.foldLeft(BsonDocument())((body, arg) => body.append(arg._1, arg._2.toBson(naming))))
      case GetField(path, input) =>
        path
          .render(naming)
          .split('.')
          .foldLeft(input.toBson(naming))((from, name) => BsonDocument("$getField", BsonDocument("field", BsonString(name)).append("input", from)))
      case Document(fields)      => fields.foldLeft(BsonDocument())((document, field) => document.append(field._1.render(naming), field._2.toBson(naming)))
      case ArrayOf(elements)     => BsonArray(elements.map(_.toBson(naming)).asJava)
      case Raw(value)            => value
  end toBson

final case class Computed[-E](path: FieldPath, expression: Expr[E, ?]):
  def toBson(naming: FieldNaming): (String, BsonValue) = (path.render(naming), expression.toBson(naming))

extension (name: String)
  @targetName("computedAs")
  infix def :=[E](expression: Expr[E, ?]): Computed[E] = Computed(FieldPath.literal(name), expression)

object Expr:

  @annotation.implicitNotFound("${A} and ${B} are not numbers MongoDB can combine, so this arithmetic does not apply to them")
  trait Widened[A, B, R]

  object Widened extends LowerWidening:
    private val instance: Widened[Any, Any, Any] = new Widened[Any, Any, Any] {}

    private[operations] def of[A, B, R]: Widened[A, B, R] = instance.asInstanceOf[Widened[A, B, R]]

    given same: [A] => (@unused numeric: NumericValue[A]) => Widened[A, A, A] = of

    given intLong: Widened[Int, Long, Long]         = of
    given longInt: Widened[Long, Int, Long]         = of
    given intDouble: Widened[Int, Double, Double]   = of
    given doubleInt: Widened[Double, Int, Double]   = of
    given longDouble: Widened[Long, Double, Double] = of
    given doubleLong: Widened[Double, Long, Double] = of

  trait LowerWidening:
    given toDecimal: [A] => (@unused numeric: NumericValue[A]) => Widened[A, BigDecimal, BigDecimal]   = Widened.of
    given fromDecimal: [A] => (@unused numeric: NumericValue[A]) => Widened[BigDecimal, A, BigDecimal] = Widened.of

  @annotation.implicitNotFound("${A} and ${B} are not values MongoDB compares as one kind, so this comparison does not apply to them")
  trait Comparison[A, B]

  object Comparison:
    private val instance: Comparison[Any, Any] = new Comparison[Any, Any] {}

    given same: [A] => Comparison[A, A]                                                 = instance.asInstanceOf[Comparison[A, A]]
    given numbers: [A, B, R] => (@unused widened: Widened[A, B, R]) => Comparison[A, B] = instance.asInstanceOf[Comparison[A, B]]

  @annotation.implicitNotFound("${A} is not text, so this string operator does not apply to it")
  trait Textual[A]

  object Textual:
    private val instance: Textual[Any] = new Textual[Any] {}

    given string: Textual[String]           = instance.asInstanceOf[Textual[String]]
    given optional: Textual[Option[String]] = instance.asInstanceOf[Textual[Option[String]]]

  @annotation.implicitNotFound("${A} is not a date, so this date operator does not apply to it")
  trait Temporal[A]

  object Temporal:
    private val instance: Temporal[Any] = new Temporal[Any] {}

    given instant: Temporal[Instant]          = instance.asInstanceOf[Temporal[Instant]]
    given optional: Temporal[Option[Instant]] = instance.asInstanceOf[Temporal[Option[Instant]]]

  @annotation.implicitNotFound("${A} cannot be read as ${B}")
  trait Unwrapped[A, B]

  object Unwrapped extends LowerUnwrapped:
    given option: [A] => Unwrapped[Option[A], A] = LowerUnwrapped.instance.asInstanceOf[Unwrapped[Option[A], A]]

  trait LowerUnwrapped:
    given identity: [A] => Unwrapped[A, A] = LowerUnwrapped.instance.asInstanceOf[Unwrapped[A, A]]

  object LowerUnwrapped:
    private[operations] val instance: Unwrapped[Any, Any] = new Unwrapped[Any, Any] {}

  private val ThisElement = "this"

  private[operations] def ambiguous(value: BsonValue): Boolean =
    value.isDocument || value.isArray || (value.isString && value.asString.getValue.startsWith("$"))

  def of[E, A, B](field: Field[E, A])(using @unused unwrapped: Unwrapped[A, B]): Expr[E, B] = FieldRef(field.path)

  def lit[A](value: A)(using encoder: BsonEncoder[A]): Expr[Any, A] = Literal(encoder.encode(value))
  def raw[A](value: BsonValue): Expr[Any, A]                        = Raw(value)

  def variable[A](name: String): Expr[Any, A] =
    require(name.matches("[a-z][A-Za-z0-9_]*"), s"a variable starts with a lowercase letter and holds only letters, digits and _, got '$name'")
    Variable(name, FieldPath.empty)

  def root[E]: Expr[E, E]     = Variable("ROOT", FieldPath.empty)
  def now: Expr[Any, Instant] = Variable("NOW", FieldPath.empty)

  val descend: Expr[Any, Nothing] = Variable("DESCEND", FieldPath.empty)
  val prune: Expr[Any, Nothing]   = Variable("PRUNE", FieldPath.empty)
  val keep: Expr[Any, Nothing]    = Variable("KEEP", FieldPath.empty)

  val searchScore: Expr[Any, Double]       = Unary("$meta", Literal(BsonString("searchScore")))
  val vectorSearchScore: Expr[Any, Double] = Unary("$meta", Literal(BsonString("vectorSearchScore")))
  val textScore: Expr[Any, Double]         = Unary("$meta", Literal(BsonString("textScore")))

  def cond[E, A](condition: Expr[E, Boolean], whenTrue: Expr[E, A], whenFalse: Expr[E, A]): Expr[E, A] =
    Named("$cond", List("if" -> condition, "then" -> whenTrue, "else" -> whenFalse))

  def switch[E, A](branches: (Expr[E, Boolean], Expr[E, A])*)(default: Expr[E, A]): Expr[E, A] =
    require(branches.nonEmpty, "a $switch needs at least one branch")
    val cases = branches.toList.map((condition, value) => Document(List(FieldPath.literal("case") -> condition, FieldPath.literal("then") -> value)))
    Named("$switch", List("branches" -> ArrayOf(cases), "default" -> default))
  end switch

  def mergeObjects[E](documents: Expr[E, ?]*): Expr[E, BsonDocument] =
    require(documents.nonEmpty, "$mergeObjects needs at least one document")
    Operator("$mergeObjects", documents.toList)

  def document[E](fields: Computed[E]*): Expr[E, BsonDocument] = Document(fields.toList.map(field => field.path -> field.expression))

  def array[E, A](elements: Expr[E, A]*): Expr[E, List[A]] = ArrayOf(elements.toList)

  extension [E, A](self: Expr[E, A])
    def equalTo[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean]    = Operator("$eq", List(self, other))
    def notEqualTo[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean] = Operator("$ne", List(self, other))
    def lt[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean]         = Operator("$lt", List(self, other))
    def lte[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean]        = Operator("$lte", List(self, other))
    def gt[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean]         = Operator("$gt", List(self, other))
    def gte[E2 <: E, B](other: Expr[E2, B])(using @unused comparison: Comparison[A, B]): Expr[E2, Boolean]        = Operator("$gte", List(self, other))

    @targetName("isEqualTo")
    infix def ===[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = equalTo(other)

    @targetName("isNotEqualTo")
    infix def =!=[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = notEqualTo(other)

    @targetName("isLessThan")
    infix def <[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = lt(other)

    @targetName("isAtMost")
    infix def <=[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = lte(other)

    @targetName("isGreaterThan")
    infix def >[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = gt(other)

    @targetName("isAtLeast")
    infix def >=[E2 <: E, B](other: Expr[E2, B])(using Comparison[A, B]): Expr[E2, Boolean] = gte(other)

    def orElse[E2 <: E, B >: A](fallback: Expr[E2, B]): Expr[E2, B] = Operator("$ifNull", List(self, fallback))

    def typeName: Expr[E, String]   = Unary("$type", self)
    def asString: Expr[E, String]   = Unary("$toString", self)
    def toInt: Expr[E, Int]         = Unary("$toInt", self)
    def toLong: Expr[E, Long]       = Unary("$toLong", self)
    def toDouble: Expr[E, Double]   = Unary("$toDouble", self)
    def toBoolean: Expr[E, Boolean] = Unary("$toBool", self)
    def toDate: Expr[E, Instant]    = Unary("$toDate", self)

    def toDecimal: Expr[E, BigDecimal] = Unary("$toDecimal", self)

    @targetName("dividedBy")
    infix def /[E2 <: E, B](other: Expr[E2, B])(using @unused dividend: NumericValue[A], @unused divisor: NumericValue[B]): Expr[E2, Double] =
      Operator("$divide", List(self, other))

    def get[B](field: Field[A, B]): Expr[E, B] =
      self match
        case FieldRef(path)       => FieldRef(path ++ field.path)
        case Variable(name, path) => Variable(name, path ++ field.path)
        case other                => GetField(field.path, other)
  end extension

  extension [E, A](self: Expr[E, A])(using @unused numeric: NumericValue[A])
    @targetName("plus")
    infix def +[E2 <: E, B, R](other: Expr[E2, B])(using @unused widened: Widened[A, B, R]): Expr[E2, R] = Operator("$add", List(self, other))

    @targetName("minus")
    infix def -[E2 <: E, B, R](other: Expr[E2, B])(using @unused widened: Widened[A, B, R]): Expr[E2, R] = Operator("$subtract", List(self, other))

    @targetName("times")
    infix def *[E2 <: E, B, R](other: Expr[E2, B])(using @unused widened: Widened[A, B, R]): Expr[E2, R] = Operator("$multiply", List(self, other))

    @targetName("remainder")
    infix def %[E2 <: E, B, R](other: Expr[E2, B])(using @unused widened: Widened[A, B, R]): Expr[E2, R] = Operator("$mod", List(self, other))

    @targetName("negated")
    def unary_- : Expr[E, A] = Operator("$multiply", List(self, Literal(BsonInt32(-1))))

    def abs: Expr[E, A]                = Unary("$abs", self)
    def ceil: Expr[E, A]               = Unary("$ceil", self)
    def floor: Expr[E, A]              = Unary("$floor", self)
    def sqrt: Expr[E, Double]          = Unary("$sqrt", self)
    def round(places: Int): Expr[E, A] = Operator("$round", List(self, Literal(BsonInt32(places))))
  end extension

  extension [E](self: Expr[E, Boolean])
    @targetName("and")
    infix def &&[E2 <: E](other: Expr[E2, Boolean]): Expr[E2, Boolean] = Operator("$and", List(self, other))

    @targetName("or")
    infix def ||[E2 <: E](other: Expr[E2, Boolean]): Expr[E2, Boolean] = Operator("$or", List(self, other))

    @targetName("not")
    def unary_! : Expr[E, Boolean] = Operator("$not", List(self))

  extension [E, A](self: Expr[E, A])(using @unused text: Textual[A])
    @targetName("concatenated")
    infix def ++[E2 <: E, B](other: Expr[E2, B])(using @unused otherText: Textual[B]): Expr[E2, String] = Operator("$concat", List(self, other))

    def toLower: Expr[E, String] = Unary("$toLower", self)
    def toUpper: Expr[E, String] = Unary("$toUpper", self)
    def trimmed: Expr[E, String] = Named("$trim", List("input" -> self))
    def length: Expr[E, Int]     = Unary("$strLenCP", self)

    def substring(start: Int, length: Int): Expr[E, String] =
      Operator("$substrCP", List(self, Literal(BsonInt32(start)), Literal(BsonInt32(length))))

    def split(delimiter: String): Expr[E, List[String]] = Operator("$split", List(self, Literal(BsonString(delimiter))))

    def matches(pattern: String, options: String = ""): Expr[E, Boolean] =
      Named("$regexMatch", List("input" -> self, "regex" -> Literal(BsonString(pattern))) ++ Option.when(options.nonEmpty)("options" -> Literal(BsonString(options))))
  end extension

  extension [E, C](self: Expr[E, C])
    def size[A](using @unused elementOf: ElementOf[C, A]): Expr[E, Int] = Unary("$size", self)

    def at[A](index: Int)(using @unused elementOf: ElementOf[C, A]): Expr[E, A] = Operator("$arrayElemAt", List(self, Literal(BsonInt32(index))))

    def contains[A](using @unused elementOf: ElementOf[C, A])[E2 <: E](value: Expr[E2, A]): Expr[E2, Boolean] = Operator("$in", List(value, self))

    def concatArrays[E2 <: E](other: Expr[E2, C]): Expr[E2, C] = Operator("$concatArrays", List(self, other))

    def filter[A](using @unused elementOf: ElementOf[C, A])(condition: Expr[Any, A] => Expr[E, Boolean]): Expr[E, C] = filterAs(ThisElement)(condition)

    def filterAs[A](name: String)(using @unused elementOf: ElementOf[C, A])(condition: Expr[Any, A] => Expr[E, Boolean]): Expr[E, C] =
      Named("$filter", List("input" -> self, "as" -> Literal(BsonString(name)), "cond" -> condition(element(name))))

    def map[A](using @unused elementOf: ElementOf[C, A])[B](transform: Expr[Any, A] => Expr[E, B]): Expr[E, List[B]] = mapAs(ThisElement)(transform)

    def mapAs[A](name: String)(using @unused elementOf: ElementOf[C, A])[B](transform: Expr[Any, A] => Expr[E, B]): Expr[E, List[B]] =
      Named("$map", List("input" -> self, "as" -> Literal(BsonString(name)), "in" -> transform(element(name))))
  end extension

  private def element[A](name: String): Expr[Any, A] =
    require(name.matches("[a-z][A-Za-z0-9_]*"), s"an element variable starts with a lowercase letter and holds only letters, digits and _, got '$name'")
    Variable(name, FieldPath.empty)

  extension [E, A](self: Expr[E, A])(using @unused date: Temporal[A])
    def year: Expr[E, Int]        = Unary("$year", self)
    def month: Expr[E, Int]       = Unary("$month", self)
    def dayOfMonth: Expr[E, Int]  = Unary("$dayOfMonth", self)
    def dayOfWeek: Expr[E, Int]   = Unary("$dayOfWeek", self)
    def hour: Expr[E, Int]        = Unary("$hour", self)
    def minute: Expr[E, Int]      = Unary("$minute", self)
    def second: Expr[E, Int]      = Unary("$second", self)
    def millisecond: Expr[E, Int] = Unary("$millisecond", self)

    def truncated(unit: DateUnit, binSize: Int = 1): Expr[E, Instant] =
      require(binSize >= 1, s"a $$dateTrunc binSize must be positive, got $binSize")
      Named("$dateTrunc", List("date" -> self, "unit" -> Literal(BsonString(unit.wireName)), "binSize" -> Literal(BsonInt32(binSize))))

    def formatAs(pattern: String, timezone: Option[String] = None): Expr[E, String] =
      Named("$dateToString", List("date" -> self, "format" -> Literal(BsonString(pattern))) ++ timezone.map(zone => "timezone" -> Literal(BsonString(zone))))

    def plus[E2 <: E](unit: DateUnit, amount: Expr[E2, Long]): Expr[E2, Instant] =
      Named("$dateAdd", List("startDate" -> self, "unit" -> Literal(BsonString(unit.wireName)), "amount" -> amount))

    def minus[E2 <: E](unit: DateUnit, amount: Expr[E2, Long]): Expr[E2, Instant] =
      Named("$dateSubtract", List("startDate" -> self, "unit" -> Literal(BsonString(unit.wireName)), "amount" -> amount))

    def until[E2 <: E, B](other: Expr[E2, B], unit: DateUnit)(using @unused otherDate: Temporal[B]): Expr[E2, Long] =
      Named("$dateDiff", List("startDate" -> self, "endDate" -> other, "unit" -> Literal(BsonString(unit.wireName))))
  end extension

end Expr
