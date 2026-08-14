package mongo4s.bson

import java.util.UUID
import java.time.Instant

import org.bson.*
import org.bson.types.{Decimal128, ObjectId}

import scala.jdk.CollectionConverters.given

trait DefaultBsonInstances:

  given BsonEncoder[BsonValue] = value => value

  given BsonEncoder[String] = value => BsonString(value)

  given BsonEncoder[Boolean] = value => BsonBoolean(value)

  given BsonEncoder[Int] = value => BsonInt32(value)

  given BsonEncoder[Long] = value => BsonInt64(value)

  given BsonEncoder[Double] = value => BsonDouble(value)

  given BsonEncoder[BigDecimal] = value => BsonDecimal128(Decimal128(value.bigDecimal))

  given BsonEncoder[Instant] = value => BsonDateTime(value.toEpochMilli)

  given BsonEncoder[UUID] = value => BsonString(value.toString)

  given BsonEncoder[ObjectId] = value => BsonObjectId(value)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Option[A]] =
    value => value.fold(BsonNull.VALUE)(encoder.encode)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[List[A]] =
    values => BsonArray(values.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Seq[A]] =
    values => BsonArray(values.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Set[A]] =
    values => BsonArray(values.toList.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Map[String, A]] = values =>
    values.foldLeft(BsonDocument()): (document, entry) =>
      document.append(entry._1, encoder.encode(entry._2))

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
    if value.isNumber
    then Right(value.asNumber.intValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Int, value))

  given BsonDecoder[Long] = value =>
    if value.isNumber
    then Right(value.asNumber.longValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Long, value))

  given BsonDecoder[Double] = value =>
    if value.isNumber
    then Right(value.asNumber.doubleValue)
    else Left(BsonError.typeMismatch(BsonTypeName.Double, value))

  given BsonDecoder[BigDecimal] = value =>
    if value.isDecimal128
    then Right(BigDecimal(value.asDecimal128.decimal128Value.bigDecimalValue))
    else if value.isNumber
    then Right(BigDecimal(value.asNumber.doubleValue))
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

  given [A](using decoder: BsonDecoder[A]): BsonDecoder[Map[String, A]] = value =>
    if value.isDocument then
      value.asDocument.entrySet.asScala.toList
        .foldRight[Either[BsonError, Map[String, A]]](Right(Map.empty)): (entry, acc) =>
          for
            tail    <- acc
            decoded <- decoder.decode(entry.getValue).left.map(BsonError.Nested(entry.getKey, _))
          yield tail.updated(entry.getKey, decoded)
    else Left(BsonError.typeMismatch(BsonTypeName.Object, value))
