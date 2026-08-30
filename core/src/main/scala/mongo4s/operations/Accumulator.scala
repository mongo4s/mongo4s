package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

enum Accumulator[E]:
  case Sum(expression: Accumulator.Expression[E])
  case Avg(expression: Accumulator.Expression[E])
  case Min(expression: Accumulator.Expression[E])
  case Max(expression: Accumulator.Expression[E])
  case First(expression: Accumulator.Expression[E])
  case Last(expression: Accumulator.Expression[E])
  case Push(expression: Accumulator.Expression[E])
  case AddToSet(expression: Accumulator.Expression[E])
  case Count()
  case Raw(document: BsonDocument)

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Sum(expression)      => BsonDocument("$sum", expression.toBson(naming))
    case Avg(expression)      => BsonDocument("$avg", expression.toBson(naming))
    case Min(expression)      => BsonDocument("$min", expression.toBson(naming))
    case Max(expression)      => BsonDocument("$max", expression.toBson(naming))
    case First(expression)    => BsonDocument("$first", expression.toBson(naming))
    case Last(expression)     => BsonDocument("$last", expression.toBson(naming))
    case Push(expression)     => BsonDocument("$push", expression.toBson(naming))
    case AddToSet(expression) => BsonDocument("$addToSet", expression.toBson(naming))
    case Count()              => BsonDocument("$sum", BsonInt32(1))
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
