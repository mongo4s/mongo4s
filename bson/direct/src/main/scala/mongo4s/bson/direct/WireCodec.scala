package mongo4s.bson.direct

import org.bson.{BsonReader, BsonWriter}

@annotation.implicitNotFound(
  "No WireCodec[${A}]. Give the type `derives WireCodec` if it is your own case class, enum or sealed trait; " +
    "an opaque type, or a type from elsewhere, needs a given written by hand — often " +
    "ScalarWireCodec[Underlying].imap(wrap)(unwrap)"
)
trait WireCodec[A] extends WireEncoder[A] with WireDecoder[A]:
  def fieldNaming: mongo4s.bson.FieldNaming = mongo4s.bson.FieldNaming.identity

  def imap[B](f: A => B)(g: B => A): WireCodec[B] =
    WireCodec.from(contramap(g), map(f))

  def iemap[B](f: A => Either[String, B])(g: B => A): WireCodec[B] =
    WireCodec.from(contramap(g), emap(f))

object WireCodec extends WireCodecLowPriorityDerivation, WireIterableLowPriorityInstances, WireCollectionInstances:
  inline def apply[A](using instance: WireCodec[A]): WireCodec[A] = instance

  def instance[A](enc: (BsonWriter, A) => Unit, dec: BsonReader => A): WireCodec[A] =
    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = enc(writer, value)
      def decode(reader: BsonReader): A              = dec(reader)

  def from[A](encoder: WireEncoder[A], decoder: WireDecoder[A]): WireCodec[A] =
    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = encoder.encode(writer, value)
      def decode(reader: BsonReader): A              = decoder.decode(reader)
      override def isAbsent(value: A): Boolean       = encoder.isAbsent(value)
      override def defaultOnMissing: Option[A]       = decoder.defaultOnMissing

  given fromEncoderAndDecoder: [A] => (encoder: WireEncoder[A], decoder: WireDecoder[A]) => WireCodec[A] =
    from(encoder, decoder)
