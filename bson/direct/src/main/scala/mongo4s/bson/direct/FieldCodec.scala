package mongo4s.bson.direct

import org.bson.{BsonReader, BsonWriter}

private[direct] trait FieldCodec[A] extends WireCodec[A]:
  def writeFields(writer: BsonWriter, value: A): Unit
  def readFields(reader: BsonReader): A
  def fieldNames: Array[String] = Array.empty
  def isEmpty: Boolean          = false
  def readEmpty: A              = throw UnsupportedOperationException("readEmpty called on a non-empty FieldCodec")

  def encode(writer: BsonWriter, value: A): Unit =
    writer.writeStartDocument()
    writeFields(writer, value)
    writer.writeEndDocument()
  end encode

  def decode(reader: BsonReader): A =
    reader.readStartDocument()
    val result = readFields(reader)
    reader.readEndDocument()
    result
  end decode

private[direct] object FieldCodec:
  def mapped[A, B](source: FieldCodec[A], f: A => B, g: B => A): FieldCodec[B] =
    new FieldCodec[B]:
      def writeFields(writer: BsonWriter, value: B): Unit = source.writeFields(writer, g(value))
      def readFields(reader: BsonReader): B               = f(source.readFields(reader))
      override def fieldNames: Array[String]              = source.fieldNames
      override def isEmpty: Boolean                       = source.isEmpty
      override def readEmpty: B                           = f(source.readEmpty)
      override def isAbsent(value: B): Boolean            = source.isAbsent(g(value))
      override def defaultOnMissing: Option[B]            = source.defaultOnMissing.map(f)
      override def fieldNaming: mongo4s.bson.FieldNaming  = source.fieldNaming
