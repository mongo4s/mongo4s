package mongo4s.bson.direct

import scala.deriving.Mirror

import org.bson.{BsonReader, BsonWriter}

trait WireCodec[A]:
  def encode(writer: BsonWriter, value: A): Unit
  def decode(reader: BsonReader): A

  def defaultOnMissing: Option[A] = None

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

object WireCodec extends WirePrimitiveInstances:
  inline def apply[A](using instance: WireCodec[A]): WireCodec[A] = instance

  def instance[A](enc: (BsonWriter, A) => Unit, dec: BsonReader => A): WireCodec[A] =
    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = enc(writer, value)
      def decode(reader: BsonReader): A              = dec(reader)

  inline given derived[A](using m: Mirror.Of[A], config: WireCodecConfig): WireCodec[A] =
    inline m match
      case p: Mirror.ProductOf[A] => WireProductDerivation.derived[A](using p, config)
      case s: Mirror.SumOf[A]     => WireSumDerivation.derived[A](using s, config)
