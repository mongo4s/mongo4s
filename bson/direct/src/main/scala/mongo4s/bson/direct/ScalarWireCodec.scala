package mongo4s.bson.direct

import org.bson.{BsonReader, BsonWriter}

import mongo4s.bson.BsonEncoder

trait ScalarWireCodec[A] extends WireCodec[A]:
  override def imap[B](f: A => B)(g: B => A): ScalarWireCodec[B] =
    ScalarWireCodec.from(contramap(g), map(f))

  override def iemap[B](f: A => Either[String, B])(g: B => A): ScalarWireCodec[B] =
    ScalarWireCodec.from(contramap(g), emap(f))

  def toBsonEncoder: BsonEncoder[A] =
    (value: A) =>
      val writer = BsonValueCapturingWriter()
      encode(writer, value)
      writer.result

object ScalarWireCodec:
  inline def apply[A](using instance: ScalarWireCodec[A]): ScalarWireCodec[A] = instance

  def instance[A](enc: (BsonWriter, A) => Unit, dec: BsonReader => A): ScalarWireCodec[A] =
    new ScalarWireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = enc(writer, value)
      def decode(reader: BsonReader): A              = dec(reader)

  def from[A](encoder: WireEncoder[A], decoder: WireDecoder[A]): ScalarWireCodec[A] =
    new ScalarWireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = encoder.encode(writer, value)
      def decode(reader: BsonReader): A              = decoder.decode(reader)
      override def isAbsent(value: A): Boolean       = encoder.isAbsent(value)
      override def defaultOnMissing: Option[A]       = decoder.defaultOnMissing
