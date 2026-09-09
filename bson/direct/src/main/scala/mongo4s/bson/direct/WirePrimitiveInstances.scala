package mongo4s.bson.direct

import java.util.UUID
import java.time.Instant

import scala.util.control.NonFatal

import org.bson.{BsonDecimal128, BsonReader, BsonString, BsonType, BsonValue}
import org.bson.codecs.{BsonValueCodec, DecoderContext}
import org.bson.types.{Decimal128, ObjectId}

import mongo4s.bson.BsonDecoder

trait WirePrimitiveInstances extends WireFallbackInstances:
  import WirePrimitiveInstances.*

  given ScalarWireCodec[String] = ScalarWireCodec.instance(
    (w, v) => w.writeString(v),
    r =>
      if r.getCurrentBsonType == BsonType.STRING
      then r.readString()
      else relaxed(r, stringDecoder),
  )

  given ScalarWireCodec[Int] = ScalarWireCodec.instance(
    (w, v) => w.writeInt32(v),
    r =>
      if r.getCurrentBsonType == BsonType.INT32
      then r.readInt32()
      else relaxed(r, intDecoder),
  )

  given ScalarWireCodec[Long] = ScalarWireCodec.instance(
    (w, v) => w.writeInt64(v),
    r =>
      if r.getCurrentBsonType == BsonType.INT64
      then r.readInt64()
      else relaxed(r, longDecoder),
  )

  given ScalarWireCodec[Double] = ScalarWireCodec.instance(
    (w, v) => w.writeDouble(v),
    r =>
      if r.getCurrentBsonType == BsonType.DOUBLE
      then r.readDouble()
      else relaxed(r, doubleDecoder),
  )

  given ScalarWireCodec[Boolean] = ScalarWireCodec.instance(
    (w, v) => w.writeBoolean(v),
    r =>
      if r.getCurrentBsonType == BsonType.BOOLEAN
      then r.readBoolean()
      else relaxed(r, booleanDecoder),
  )

  given ScalarWireCodec[Instant] = ScalarWireCodec.instance(
    (w, v) => w.writeDateTime(v.toEpochMilli),
    r =>
      if r.getCurrentBsonType == BsonType.DATE_TIME
      then Instant.ofEpochMilli(r.readDateTime())
      else relaxed(r, instantDecoder),
  )

  given ScalarWireCodec[ObjectId] = ScalarWireCodec.instance(
    (w, v) => w.writeObjectId(v),
    r =>
      if r.getCurrentBsonType == BsonType.OBJECT_ID
      then r.readObjectId()
      else relaxed(r, objectIdDecoder),
  )

  given ScalarWireCodec[BigDecimal] = ScalarWireCodec.instance(
    (w, v) => w.writeDecimal128(Decimal128(v.bigDecimal)),
    r =>
      if r.getCurrentBsonType == BsonType.DECIMAL128
      then
        val decimal = r.readDecimal128()
        try BigDecimal(decimal.bigDecimalValue)
        catch case NonFatal(_) => decode(BsonDecimal128(decimal), bigDecimalDecoder)
      else relaxed(r, bigDecimalDecoder),
  )

  given ScalarWireCodec[UUID] = ScalarWireCodec.instance(
    (w, v) => w.writeString(v.toString),
    r =>
      if r.getCurrentBsonType == BsonType.STRING
      then
        val raw = r.readString()
        try UUID.fromString(raw)
        catch case NonFatal(_) => decode(BsonString(raw), uuidDecoder)
      else relaxed(r, uuidDecoder),
  )

object WirePrimitiveInstances:
  private val valueCodec     = BsonValueCodec()
  private val decoderContext = DecoderContext.builder().build()

  private[direct] val stringDecoder: BsonDecoder[String]         = BsonDecoder[String]
  private[direct] val intDecoder: BsonDecoder[Int]               = BsonDecoder[Int]
  private[direct] val longDecoder: BsonDecoder[Long]             = BsonDecoder[Long]
  private[direct] val doubleDecoder: BsonDecoder[Double]         = BsonDecoder[Double]
  private[direct] val booleanDecoder: BsonDecoder[Boolean]       = BsonDecoder[Boolean]
  private[direct] val instantDecoder: BsonDecoder[Instant]       = BsonDecoder[Instant]
  private[direct] val objectIdDecoder: BsonDecoder[ObjectId]     = BsonDecoder[ObjectId]
  private[direct] val bigDecimalDecoder: BsonDecoder[BigDecimal] = BsonDecoder[BigDecimal]
  private[direct] val uuidDecoder: BsonDecoder[UUID]             = BsonDecoder[UUID]

  private[direct] def decode[A](value: BsonValue, decoder: BsonDecoder[A]): A =
    decoder.decode(value) match
      case Right(decoded) => decoded
      case Left(error)    => throw error.toThrowable

  private[direct] def relaxed[A](reader: BsonReader, decoder: BsonDecoder[A]): A =
    decode(valueCodec.decode(reader, decoderContext), decoder)
