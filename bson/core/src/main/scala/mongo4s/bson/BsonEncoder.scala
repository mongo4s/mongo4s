package mongo4s.bson

import org.bson.BsonValue

trait BsonEncoder[A]:
  def encode(value: A): BsonValue

  final def contramap[B](f: B => A): BsonEncoder[B] =
    (value: B) => encode(f(value))

object BsonEncoder:
  inline def apply[A](using instance: BsonEncoder[A]): BsonEncoder[A] = instance
