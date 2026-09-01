package mongo4s.bson

import org.bson.BsonValue

enum BsonError:
  case TypeMismatch(expected: BsonTypeName, actual: BsonTypeName)
  case MissingField(name: String)
  case InvalidValue(reason: String)
  case Nested(field: String, cause: BsonError)
  case Custom(reason: String)
  case Thrown(reason: String, cause: Throwable)

  def message: String = this match
    case TypeMismatch(expected, actual) => s"Expected ${expected.wireName} but got ${actual.wireName}"
    case MissingField(name)             => s"Missing field: $name"
    case InvalidValue(reason)           => reason
    case Nested(field, cause)           => s"$field -> ${cause.message}"
    case Custom(reason)                 => reason
    case Thrown(reason, _)              => reason

  def toThrowable: Throwable = this match
    case Thrown(_, cause) => cause
    case other            => BsonError.DecodingFailure(other)

object BsonError:
  final class DecodingFailure(val error: BsonError) extends RuntimeException(error.message, null, true, false)

  def typeMismatch(expected: BsonTypeName, actual: BsonValue): BsonError =
    TypeMismatch(expected, BsonTypeName.of(actual))

  def fromThrowable(cause: Throwable): BsonError =
    Thrown(
      Option(cause.getMessage).getOrElse(cause.getClass.getName),
      cause
    )

  def fromMessage(reason: String): BsonError = Custom(reason)
