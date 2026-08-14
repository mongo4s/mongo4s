package mongo4s.bson

import org.bson.BsonValue

trait BsonCodec[A] extends BsonEncoder[A], BsonDecoder[A]

object BsonCodec:
  inline def apply[A](using instance: BsonCodec[A]): BsonCodec[A] = instance

  def from[A](encoder: BsonEncoder[A], decoder: BsonDecoder[A]): BsonCodec[A] =
    new BsonCodec[A]:
      def encode(value: A): BsonValue                   = encoder.encode(value)
      def decode(bson: BsonValue): Either[BsonError, A] = decoder.decode(bson)
