package mongo4s.bson

import org.bson.BsonValue

trait BsonEncoder[A]:
  def encode(value: A): BsonValue

  final def contramap[B](f: B => A): BsonEncoder[B] =
    (value: B) => encode(f(value))

object BsonEncoder extends DefaultBsonEncoderInstances, BsonEncoderFromSource:
  inline def apply[A](using instance: BsonEncoder[A]): BsonEncoder[A] = instance

private[bson] trait BsonEncoderFromSource:
  given fromCodecSource: [A] => (source: BsonCodecSource[A]) => BsonEncoder[A] = source.toBsonEncoder
