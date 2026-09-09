package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

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

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Raw(document) => document
    case operator      => BsonDocument(key, operator.payload(naming))

  private def payload(naming: FieldNaming): BsonValue = this match
    case Sum(expression)      => expression.toBson(naming)
    case Avg(expression)      => expression.toBson(naming)
    case Min(expression)      => expression.toBson(naming)
    case Max(expression)      => expression.toBson(naming)
    case First(expression)    => expression.toBson(naming)
    case Last(expression)     => expression.toBson(naming)
    case Push(expression)     => expression.toBson(naming)
    case AddToSet(expression) => expression.toBson(naming)
    case Count()              => BsonInt32(1)
    case Raw(document)        => document

object Accumulator:

  enum Expression[E]:
    case FieldRef(path: FieldPath)
    case Literal(value: BsonValue)

    def toBson(naming: FieldNaming): BsonValue = this match
      case FieldRef(path) => BsonString("$" + path.render(naming))
      case Literal(value) => value

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
  def raw[E](document: BsonDocument): Accumulator[E]     = Raw(document)
