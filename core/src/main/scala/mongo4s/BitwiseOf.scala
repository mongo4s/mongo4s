package mongo4s

import org.bson.{BsonInt32, BsonInt64, BsonValue}

@annotation.implicitNotFound("${C} is not an integer field type holding ${A}, so this bitwise operator does not apply to it")
trait BitwiseOf[C, A]:
  def encode(value: A): BsonValue

object BitwiseOf:
  given int: BitwiseOf[Int, Int]                    = BsonInt32(_)
  given long: BitwiseOf[Long, Long]                 = BsonInt64(_)
  given optionalInt: BitwiseOf[Option[Int], Int]    = BsonInt32(_)
  given optionalLong: BitwiseOf[Option[Long], Long] = BsonInt64(_)
