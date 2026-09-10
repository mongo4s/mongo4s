package mongo4s

import java.util.UUID
import java.time.Instant

import org.bson.BsonValue
import org.bson.types.ObjectId

import mongo4s.bson.BsonEncoder

@annotation.implicitNotFound("${A} is not a type MongoDB orders, so this operator does not apply to it")
trait ComparableValue[A]:
  def encode(value: A): BsonValue

object ComparableValue:
  def apply[A](using instance: ComparableValue[A]): instance.type = instance

  private def of[A](using encoder: BsonEncoder[A]): ComparableValue[A] = encoder.encode(_)

  import mongo4s.bson.BsonInstances.given

  given int: ComparableValue[Int]               = of
  given long: ComparableValue[Long]             = of
  given double: ComparableValue[Double]         = of
  given bigDecimal: ComparableValue[BigDecimal] = of
  given string: ComparableValue[String]         = of
  given boolean: ComparableValue[Boolean]       = of
  given instant: ComparableValue[Instant]       = of
  given objectId: ComparableValue[ObjectId]     = of
  given uuid: ComparableValue[UUID]             = of

@annotation.implicitNotFound("${C} is not a field MongoDB orders against ${A}, so this operator does not apply to it")
trait ComparableOf[C, A]:
  def encode(value: A): BsonValue

object ComparableOf:
  given direct: [A] => (comparable: ComparableValue[A]) => ComparableOf[A, A] = comparable.encode(_)

  given optional: [A] => (comparable: ComparableValue[A]) => ComparableOf[Option[A], A] = comparable.encode(_)
