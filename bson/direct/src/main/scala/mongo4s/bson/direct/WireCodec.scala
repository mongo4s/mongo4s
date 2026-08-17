package mongo4s.bson.direct

import scala.deriving.Mirror

import org.bson.{BsonReader, BsonWriter}

trait WireCodec[A] extends WireEncoder[A] with WireDecoder[A]:
  def imap[B](f: A => B)(g: B => A): WireCodec[B] =
    WireCodec.from(contramap(g), map(f))

  def iemap[B](f: A => Either[String, B])(g: B => A): WireCodec[B] =
    WireCodec.from(contramap(g), emap(f))

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

private[direct] trait WireCodecLowPriorityDerivation:
  inline given derived[A](using m: Mirror.Of[A], config: WireCodecConfig): WireCodec[A] =
    inline m match
      case p: Mirror.ProductOf[A] => WireProductDerivation.derived[A](using p, config)
      case s: Mirror.SumOf[A]     => WireSumDerivation.derived[A](using s, config)

object WireCodec extends WireCodecLowPriorityDerivation, WirePrimitiveInstances:
  inline def apply[A](using instance: WireCodec[A]): WireCodec[A] = instance

  def instance[A](enc: (BsonWriter, A) => Unit, dec: BsonReader => A): WireCodec[A] =
    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = enc(writer, value)
      def decode(reader: BsonReader): A              = dec(reader)

  def from[A](encoder: WireEncoder[A], decoder: WireDecoder[A]): WireCodec[A] =
    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = encoder.encode(writer, value)
      def decode(reader: BsonReader): A              = decoder.decode(reader)
      override def defaultOnMissing: Option[A]       = decoder.defaultOnMissing

  given fromEncoderAndDecoder[A](using encoder: WireEncoder[A], decoder: WireDecoder[A]): WireCodec[A] =
    from(encoder, decoder)
