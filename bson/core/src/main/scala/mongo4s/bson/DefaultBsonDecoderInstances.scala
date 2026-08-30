package mongo4s.bson

import java.util.UUID
import java.time.Instant

import org.bson.*
import org.bson.types.ObjectId

import scala.jdk.CollectionConverters.given

trait DefaultBsonDecoderInstances:

  private def wholeNumber(value: BsonValue, target: String): Either[BsonError, Long] =
    val asDouble = value.asNumber.doubleValue
    if asDouble != Math.rint(asDouble)
    then Left(BsonError.InvalidValue(s"$asDouble is not a whole number and cannot be read as $target"))
    else if asDouble < Long.MinValue.toDouble || asDouble > Long.MaxValue.toDouble
    then Left(BsonError.InvalidValue(s"$asDouble is out of range for $target"))
    else Right(asDouble.toLong)

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
      wholeNumber(value, "Int").flatMap: whole =>
        if whole < Int.MinValue || whole > Int.MaxValue
        then Left(BsonError.InvalidValue(s"$whole is out of range for Int"))
        else Right(whole.toInt)

  given BsonDecoder[Long] = value =>
    if !value.isNumber
    then Left(BsonError.typeMismatch(BsonTypeName.Long, value))
    else if value.isInt32 || value.isInt64
    then Right(value.asNumber.longValue)
    else wholeNumber(value, "Long")

  given BsonDecoder[Double] = value =>
    if value.isNumber
    then Right(value.asNumber.doubleValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Double, value))

  given BsonDecoder[BigDecimal] = value =>
    if value.isDecimal128 then Right(BigDecimal(value.asDecimal128.decimal128Value.bigDecimalValue))
    else if value.isInt32 then Right(BigDecimal(value.asInt32.getValue))
    else if value.isInt64 then Right(BigDecimal(value.asInt64.getValue))
    else if value.isDouble then Right(BigDecimal(value.asDouble.getValue))
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
    else Left(BsonError.typeMismatch(BsonTypeName.String, value))

  given BsonDecoder[ObjectId] = value =>
    if value.isObjectId
    then Right(value.asObjectId.getValue)
    else Left(BsonError.typeMismatch(BsonTypeName.ObjectId, value))

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Option[A]] = value => if value.isNull then Right(None) else decoder.decode(value).map(Some(_))

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[List[A]] = value =>
    if value.isArray then
      value.asArray.getValues.asScala.toList
        .foldRight[Either[BsonError, List[A]]](Right(Nil)): (element, acc) =>
          for
            tail <- acc
            head <- decoder.decode(element)
          yield head :: tail
    else Left(BsonError.typeMismatch(BsonTypeName.Array, value))

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Vector[A]] = value => summon[BsonDecoder[List[A]]].decode(value).map(_.toVector)

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Seq[A]] = value => summon[BsonDecoder[List[A]]].decode(value)

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Set[A]] = value => summon[BsonDecoder[List[A]]].decode(value).map(_.toSet)

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Map[String, A]] = value =>
    if value.isDocument then
      value.asDocument.entrySet.asScala.toList
        .foldRight[Either[BsonError, Map[String, A]]](Right(Map.empty)): (entry, acc) =>
          for
            tail    <- acc
            decoded <- decoder.decode(entry.getValue).left.map(BsonError.Nested(entry.getKey, _))
          yield tail.updated(entry.getKey, decoded)
    else Left(BsonError.typeMismatch(BsonTypeName.Object, value))
