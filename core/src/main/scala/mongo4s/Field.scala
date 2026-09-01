package mongo4s

import scala.annotation.targetName

import mongo4s.bson.BsonEncoder
import mongo4s.operations.{Filter, PushOptions, Sort, Update}

opaque type Field[E, A] = FieldPath

object Field:
  def apply[E, A](path: FieldPath): Field[E, A] = path

  inline def of[E, A](inline selector: E => A): Field[E, A] = ${ FieldMacro.impl[E, A]('selector) }

  def stored[E, A](path: String): Field[E, A] = Field(FieldPath.literal(path))

  extension [E, A](field: Field[E, A]) def path: FieldPath = field

  extension [E, A](field: Field[E, A])(using encoder: BsonEncoder[A])
    def equalTo(value: A): Filter[E]    = Filter.Eq(field.path, encoder.encode(value))
    def notEqualTo(value: A): Filter[E] = Filter.Ne(field.path, encoder.encode(value))
    def gt(value: A): Filter[E]         = Filter.Gt(field.path, encoder.encode(value))
    def gte(value: A): Filter[E]        = Filter.Gte(field.path, encoder.encode(value))
    def lt(value: A): Filter[E]         = Filter.Lt(field.path, encoder.encode(value))
    def lte(value: A): Filter[E]        = Filter.Lte(field.path, encoder.encode(value))

    @targetName("isEqualTo")
    infix def ===(value: A): Filter[E] = equalTo(value)

    @targetName("isNotEqualTo")
    infix def =!=(value: A): Filter[E] = notEqualTo(value)

    @targetName("isGreaterThan")
    infix def >(value: A): Filter[E] = gt(value)

    @targetName("isGreaterThanOrEqualTo")
    infix def >=(value: A): Filter[E] = gte(value)

    @targetName("isLessThan")
    infix def <(value: A): Filter[E] = lt(value)

    @targetName("isLessThanOrEqualTo")
    infix def <=(value: A): Filter[E] = lte(value)

    def in(values: Seq[A]): Filter[E] =
      if values.isEmpty then Filter.MatchNone() else Filter.In(field.path, values.toList.map(encoder.encode))

    def notIn(values: Seq[A]): Filter[E] =
      if values.isEmpty then Filter.MatchAll() else Filter.Nin(field.path, values.toList.map(encoder.encode))

    def set(value: A): Update[E]         = Update.Set(field.path, encoder.encode(value))
    def setOnInsert(value: A): Update[E] = Update.SetOnInsert(field.path, encoder.encode(value))

  extension [E, V](field: Field[E, Map[String, V]]) def at(key: String): Field[E, V] = Field(field.path.stored(key))

  extension [E, C](field: Field[E, C])
    @targetName("child")
    infix def /[A](segment: String): Field[E, A] = Field(field.path.stored(segment))

  extension [E, A](field: Field[E, A])
    def exists: Filter[E]                                       = Filter.Exists(field.path, true)
    def notExists: Filter[E]                                    = Filter.Exists(field.path, false)
    def regex(pattern: String, options: String = ""): Filter[E] = Filter.Regex(field.path, pattern, options)
    def unset: Update[E]                                        = Update.Unset(field.path)
    def ascending: Sort[E]                                      = Sort.asc(field)
    def descending: Sort[E]                                     = Sort.desc(field)

  extension [E, C](field: Field[E, C])
    def inc[A](amount: A)(using NumericOf[C, A]): Update[E] = Update.inc(field, amount)
    def mul[A](factor: A)(using NumericOf[C, A]): Update[E] = Update.mul(field, factor)
    def min[A](value: A)(using NumericOf[C, A]): Update[E]  = Update.min(field, value)
    def max[A](value: A)(using NumericOf[C, A]): Update[E]  = Update.max(field, value)

    def push[A](value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E] = Update.push(field, value)

    def pushAll[A](values: Seq[A], options: PushOptions[A] = PushOptions.default[A])(using
        ElementOf[C, A],
        BsonEncoder[A],
    ): Update[E] = Update.pushAll(field, values, options)

    def pull[A](value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E]     = Update.pull(field, value)
    def addToSet[A](value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E] = Update.addToSet(field, value)

    def elemMatch[A](filter: Filter[A])(using ElementOf[C, A]): Filter[E]                    =
      Filter.ElemMatch(field.path, filter)
    def contains[A](value: A)(using ev: ElementOf[C, A], encoder: BsonEncoder[A]): Filter[E] =
      Filter.Eq(field.path, encoder.encode(value))

    def containsAll[A](values: Seq[A])(using ev: ElementOf[C, A], encoder: BsonEncoder[A]): Filter[E] =
      if values.isEmpty
      then Filter.MatchAll()
      else Filter.All(field.path, values.toList.map(encoder.encode))

    def hasSize(size: Int): Filter[E] = Filter.Size(field.path, size)

  extension [E, A](field: Field[E, A])
    def hasType(bsonType: String): Filter[E]           = Filter.Type(field.path, bsonType)
    def mod(divisor: Long, remainder: Long): Filter[E] = Filter.Mod(field.path, divisor, remainder)
