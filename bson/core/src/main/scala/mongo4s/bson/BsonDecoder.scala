package mongo4s.bson

import org.bson.BsonValue

trait BsonDecoder[A]:
  def decode(bson: BsonValue): Either[BsonError, A]

  final def map[B](f: A => B): BsonDecoder[B] =
    (bson: BsonValue) => decode(bson).map(f)

  final def emap[B](f: A => Either[BsonError, B]): BsonDecoder[B] =
    (bson: BsonValue) => decode(bson).flatMap(f)

object BsonDecoder:
  inline def apply[A](using instance: BsonDecoder[A]): BsonDecoder[A] = instance
