package mongo4s.bson.direct

import org.bson.{BsonReader, BsonType, BsonWriter}

trait WirePrimitiveInstances extends WireFallbackInstances:

  given WireCodec[String]  = WireCodec.instance((w, v) => w.writeString(v), r => r.readString())
  given WireCodec[Int]     = WireCodec.instance((w, v) => w.writeInt32(v), r => r.readInt32())
  given WireCodec[Long]    = WireCodec.instance((w, v) => w.writeInt64(v), r => r.readInt64())
  given WireCodec[Double]  = WireCodec.instance((w, v) => w.writeDouble(v), r => r.readDouble())
  given WireCodec[Boolean] = WireCodec.instance((w, v) => w.writeBoolean(v), r => r.readBoolean())

  given optionWireCodec[A](using inner: WireCodec[A]): WireCodec[Option[A]] with
    def encode(writer: BsonWriter, value: Option[A]): Unit = value match
      case Some(a) => inner.encode(writer, a)
      case None    => writer.writeNull()

    def decode(reader: BsonReader): Option[A] =
      if reader.getCurrentBsonType == BsonType.NULL then
        reader.readNull()
        None
      else Some(inner.decode(reader))

    override def defaultOnMissing: Option[Option[A]] = Some(None)

  given listWireCodec[A](using inner: WireCodec[A]): WireCodec[List[A]] with
    def encode(writer: BsonWriter, values: List[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): List[A] =
      reader.readStartArray()
      val builder = List.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()
