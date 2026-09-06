package mongo4s.bson.direct

import java.util.UUID
import java.time.Instant

import org.bson.{BsonReader, BsonType}
import org.bson.types.{Decimal128, ObjectId}

import mongo4s.bson.{BsonError, BsonTypeName}

trait WirePrimitiveInstances extends WireFallbackInstances:

  private def mismatch(expected: BsonTypeName, actual: BsonType): Nothing =
    throw BsonError.DecodingFailure(BsonError.TypeMismatch(expected, BsonTypeName.of(actual)))

  private def decimalOf(decimal: Decimal128): BigDecimal =
    if decimal.isNaN || decimal.isInfinite
    then throw BsonError.DecodingFailure(BsonError.InvalidValue(s"$decimal has no decimal value and cannot be read as BigDecimal"))
    else
      try BigDecimal(decimal.bigDecimalValue)
      catch case _: ArithmeticException => BigDecimal(0)

  private def readBigDecimal(reader: BsonReader): BigDecimal =
    reader.getCurrentBsonType match
      case BsonType.DECIMAL128 => decimalOf(reader.readDecimal128())
      case BsonType.INT32      => BigDecimal(reader.readInt32())
      case BsonType.INT64      => BigDecimal(reader.readInt64())
      case BsonType.DOUBLE     => BigDecimal(reader.readDouble())
      case other               => mismatch(BsonTypeName.Decimal, other)

  private def readInstant(reader: BsonReader): Instant =
    reader.getCurrentBsonType match
      case BsonType.DATE_TIME => Instant.ofEpochMilli(reader.readDateTime())
      case BsonType.TIMESTAMP => Instant.ofEpochSecond(reader.readTimestamp().getTime.toLong)
      case other              => mismatch(BsonTypeName.Date, other)

  private def readUuid(reader: BsonReader): UUID =
    reader.getCurrentBsonType match
      case BsonType.STRING =>
        val raw = reader.readString()
        try UUID.fromString(raw)
        catch
          case error: IllegalArgumentException =>
            throw BsonError.DecodingFailure(BsonError.InvalidValue(s"Invalid UUID: ${error.getMessage}"))
      case other           => mismatch(BsonTypeName.String, other)

  private def readObjectId(reader: BsonReader): ObjectId =
    reader.getCurrentBsonType match
      case BsonType.OBJECT_ID => reader.readObjectId()
      case other              => mismatch(BsonTypeName.ObjectId, other)

  given ScalarWireCodec[String]  = ScalarWireCodec.instance((w, v) => w.writeString(v), r => r.readString())
  given ScalarWireCodec[Int]     = ScalarWireCodec.instance((w, v) => w.writeInt32(v), r => r.readInt32())
  given ScalarWireCodec[Long]    = ScalarWireCodec.instance((w, v) => w.writeInt64(v), r => r.readInt64())
  given ScalarWireCodec[Double]  = ScalarWireCodec.instance((w, v) => w.writeDouble(v), r => r.readDouble())
  given ScalarWireCodec[Boolean] = ScalarWireCodec.instance((w, v) => w.writeBoolean(v), r => r.readBoolean())

  given ScalarWireCodec[BigDecimal] =
    ScalarWireCodec.instance((w, v) => w.writeDecimal128(Decimal128(v.bigDecimal)), readBigDecimal)

  given ScalarWireCodec[Instant] =
    ScalarWireCodec.instance((w, v) => w.writeDateTime(v.toEpochMilli), readInstant)

  given ScalarWireCodec[UUID] =
    ScalarWireCodec.instance((w, v) => w.writeString(v.toString), readUuid)

  given ScalarWireCodec[ObjectId] =
    ScalarWireCodec.instance((w, v) => w.writeObjectId(v), readObjectId)
