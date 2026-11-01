package mongo4s.bson

import java.util.UUID
import java.time.Instant

import scala.annotation.tailrec
import scala.collection.mutable

import org.bson.*
import org.bson.types.{Decimal128, ObjectId}

trait DefaultBsonDecoderInstances:

  private def wholeNumber(value: BsonValue, target: String): Either[BsonError, Long] =
    val asDouble = value.asNumber.doubleValue
    if asDouble != Math.rint(asDouble)
    then Left(BsonError.InvalidValue(s"$asDouble is not a whole number and cannot be read as $target"))
    else if asDouble < Long.MinValue.toDouble || asDouble >= Long.MaxValue.toDouble
    then Left(BsonError.InvalidValue(s"$asDouble is out of range for $target"))
    else Right(asDouble.toLong)

  private def bigDecimalOf(decimal: Decimal128, target: String): Either[BsonError, BigDecimal] =
    if decimal.isNaN || decimal.isInfinite
    then Left(BsonError.InvalidValue(s"$decimal has no decimal value and cannot be read as $target"))
    else
      try Right(BigDecimal(decimal.bigDecimalValue))
      catch case _: ArithmeticException => Right(BigDecimal(0))

  private def bigDecimalOf(double: Double, target: String): Either[BsonError, BigDecimal] =
    if double.isNaN || double.isInfinite
    then Left(BsonError.InvalidValue(s"$double has no decimal value and cannot be read as $target"))
    else Right(BigDecimal(double))

  private def wholeDecimal(decimal: BigDecimal, target: String): Either[BsonError, Long] =
    if !decimal.isWhole
    then Left(BsonError.InvalidValue(s"$decimal is not a whole number and cannot be read as $target"))
    else if !decimal.isValidLong
    then Left(BsonError.InvalidValue(s"$decimal is out of range for $target"))
    else Right(decimal.toLongExact)

  private def longNumber(value: BsonValue, target: String): Either[BsonError, Long] =
    if value.isDecimal128
    then bigDecimalOf(value.asDecimal128.decimal128Value, target).flatMap(wholeDecimal(_, target))
    else wholeNumber(value, target)

  given BsonDecoder[BsonValue] = value => Right(value)

  given BsonDecoder[String] = value =>
    if value.isString
    then Right(value.asString.getValue)
    else Left(BsonError.typeMismatch(BsonTypeName.String, value))

  given BsonDecoder[Boolean] = value =>
    if value.isBoolean
    then Right(value.asBoolean.getValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Bool, value))

  given BsonDecoder[Int] = value =>
    if !value.isNumber
    then Left(BsonError.typeMismatch(BsonTypeName.Int, value))
    else if value.isInt32
    then Right(value.asInt32.getValue)
    else
      longNumber(value, "Int").flatMap { whole =>
        if whole < Int.MinValue || whole > Int.MaxValue
        then Left(BsonError.InvalidValue(s"$whole is out of range for Int"))
        else Right(whole.toInt)
      }

  given BsonDecoder[Long] = value =>
    if !value.isNumber
    then Left(BsonError.typeMismatch(BsonTypeName.Long, value))
    else if value.isInt32 || value.isInt64
    then Right(value.asNumber.longValue)
    else longNumber(value, "Long")

  given BsonDecoder[Double] = value =>
    if value.isNumber
    then Right(value.asNumber.doubleValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Double, value))

  given BsonDecoder[BigDecimal] = value =>
    if value.isDecimal128
    then bigDecimalOf(value.asDecimal128.decimal128Value, "BigDecimal")
    else if value.isInt32
    then Right(BigDecimal(value.asInt32.getValue))
    else if value.isInt64
    then Right(BigDecimal(value.asInt64.getValue))
    else if value.isDouble
    then bigDecimalOf(value.asDouble.getValue, "BigDecimal")
    else Left(BsonError.typeMismatch(BsonTypeName.Decimal, value))

  given BsonDecoder[Instant] = value =>
    if value.isDateTime
    then Right(Instant.ofEpochMilli(value.asDateTime.getValue))
    else if value.isTimestamp
    then Right(Instant.ofEpochSecond(value.asTimestamp.getTime.toLong))
    else Left(BsonError.typeMismatch(BsonTypeName.Date, value))

  given BsonDecoder[UUID] = value =>
    if value.isString
    then
      try Right(UUID.fromString(value.asString.getValue))
      catch case error: IllegalArgumentException => Left(BsonError.InvalidValue(s"Invalid UUID: ${error.getMessage}"))
    else if value.isBinary && value.asBinary.getType == BsonBinarySubType.UUID_STANDARD.getValue
    then
      try Right(value.asBinary.asUuid())
      catch case error: BsonInvalidOperationException => Left(BsonError.InvalidValue(s"Invalid UUID: ${error.getMessage}"))
    else if value.isBinary && value.asBinary.getType == BsonBinarySubType.UUID_LEGACY.getValue
    then Left(BsonError.InvalidValue("A legacy UUID (binary subtype 3) does not say which byte order wrote it, so it is not read"))
    else Left(BsonError.typeMismatch(BsonTypeName.String, value))

  given BsonDecoder[ObjectId] = value =>
    if value.isObjectId
    then Right(value.asObjectId.getValue)
    else Left(BsonError.typeMismatch(BsonTypeName.ObjectId, value))

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[Option[A]] = value =>
    if value.isNull
    then Right(None)
    else decoder.decode(value).map(Some(_))

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[List[A]] = elements(_, decoder, List.newBuilder[A])

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[Vector[A]] = elements(_, decoder, Vector.newBuilder[A])

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[Seq[A]] = elements(_, decoder, List.newBuilder[A])

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[Set[A]] = elements(_, decoder, Set.newBuilder[A])

  given [A] => (decoder: BsonDecoder[A]) => BsonDecoder[Map[String, A]] = value =>
    if value.isDocument
    then
      val entries = value.asDocument.entrySet.iterator
      val decoded = Map.newBuilder[String, A]

      @tailrec def loop(): Either[BsonError, Map[String, A]] =
        if !entries.hasNext
        then Right(decoded.result())
        else
          val entry = entries.next()
          decoder.decode(entry.getValue) match
            case Right(element) =>
              decoded.addOne((entry.getKey, element)): Unit
              loop()
            case Left(error)    => Left(BsonError.Nested(entry.getKey, error))

      loop()
    else Left(BsonError.typeMismatch(BsonTypeName.Object, value))

  private def elements[A, C](value: BsonValue, decoder: BsonDecoder[A], decoded: mutable.Builder[A, C]): Either[BsonError, C] =
    if value.isArray
    then
      val values = value.asArray.getValues
      decoded.sizeHint(values.size)

      @tailrec def loop(index: Int): Either[BsonError, C] =
        if index == values.size
        then Right(decoded.result())
        else
          decoder.decode(values.get(index)) match
            case Right(element) =>
              decoded.addOne(element): Unit
              loop(index + 1)
            case Left(error)    => Left(BsonError.Nested(index.toString, error))

      loop(0)
    else Left(BsonError.typeMismatch(BsonTypeName.Array, value))
