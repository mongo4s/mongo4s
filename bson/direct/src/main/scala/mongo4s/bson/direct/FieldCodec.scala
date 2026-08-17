package mongo4s.bson.direct

import org.bson.{BsonReader, BsonWriter}

private[direct] trait FieldCodec[A] extends WireCodec[A]:
  def writeFields(writer: BsonWriter, value: A): Unit
  def readFields(reader: BsonReader): A
  def isEmpty: Boolean = false
  def readEmpty: A     = throw UnsupportedOperationException("readEmpty called on a non-empty FieldCodec")

  def encode(writer: BsonWriter, value: A): Unit =
    writer.writeStartDocument()
    writeFields(writer, value)
    writer.writeEndDocument()

  def decode(reader: BsonReader): A =
    reader.readStartDocument()
    val result = readFields(reader)
    reader.readEndDocument()
    result
