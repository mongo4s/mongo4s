package mongo4s.operations

import scala.annotation.unused

import org.bson.{BsonDocument, BsonString, BsonValue}

import mongo4s.{Field, FieldPath, NumericOf}
import mongo4s.bson.BsonEncoder

enum FillOutput[E]:
  case Value(path: FieldPath, value: BsonValue)
  case Linear(path: FieldPath)
  case Locf(path: FieldPath)

  def path: FieldPath

  def needsSort: Boolean = this match
    case Value(_, _) => false
    case _           => true

  def toBson: BsonDocument = this match
    case Value(_, value) => BsonDocument("value", value)
    case Linear(_)       => BsonDocument("method", BsonString("linear"))
    case Locf(_)         => BsonDocument("method", BsonString("locf"))

object FillOutput:
  def value[E, A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): FillOutput[E] = Value(field.path, encoder.encode(value))

  def linear[E, A](field: Field[E, A])(using @unused numeric: NumericOf[A, ?]): FillOutput[E] = Linear(field.path)

  def locf[E, A](field: Field[E, A]): FillOutput[E] = Locf(field.path)
