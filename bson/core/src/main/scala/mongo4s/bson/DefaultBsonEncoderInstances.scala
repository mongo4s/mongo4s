package mongo4s.bson

import java.util.UUID
import java.time.Instant

import org.bson.*
import org.bson.types.{Decimal128, ObjectId}

import scala.jdk.CollectionConverters.given

trait DefaultBsonEncoderInstances:

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

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Vector[A]] =
    values => BsonArray(values.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Seq[A]] =
    values => BsonArray(values.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Set[A]] =
    values => BsonArray(values.toList.map(encoder.encode).asJava)

  given [A](using encoder: BsonEncoder[A]): BsonEncoder[Map[String, A]] = values =>
    values.foldLeft(BsonDocument()): (document, entry) =>
      document.append(entry._1, encoder.encode(entry._2))
