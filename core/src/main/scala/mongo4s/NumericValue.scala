package mongo4s

import org.bson.types.Decimal128
import org.bson.{BsonDecimal128, BsonDouble, BsonInt32, BsonInt64, BsonValue}

@annotation.implicitNotFound("${A} is not a numeric field type, so this operator does not apply to it")
trait NumericValue[A]:
  def encode(value: A): BsonValue

object NumericValue:
  def apply[A](using instance: NumericValue[A]): instance.type = instance

  given int: NumericValue[Int]               = value => BsonInt32(value)
  given long: NumericValue[Long]             = value => BsonInt64(value)
  given double: NumericValue[Double]         = value => BsonDouble(value)
  given bigDecimal: NumericValue[BigDecimal] = value => BsonDecimal128(Decimal128(value.bigDecimal))

@annotation.implicitNotFound("${C} is not a numeric field type holding ${A}, so this operator does not apply to it")
trait NumericOf[C, A]:
  def encode(value: A): BsonValue

object NumericOf:
  given direct[A](using numeric: NumericValue[A]): NumericOf[A, A] = numeric.encode(_)

  given optional[A](using numeric: NumericValue[A]): NumericOf[Option[A], A] = numeric.encode(_)
