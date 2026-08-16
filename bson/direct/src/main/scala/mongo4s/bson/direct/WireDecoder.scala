package mongo4s.bson.direct

import org.bson.BsonReader

import mongo4s.bson.BsonError

trait WireDecoder[A]:
  def decode(reader: BsonReader): A

  def defaultOnMissing: Option[A] = None

  def map[B](f: A => B): WireDecoder[B] =
    new WireDecoder[B]:
      def decode(reader: BsonReader): B        = f(WireDecoder.this.decode(reader))
      override def defaultOnMissing: Option[B] = WireDecoder.this.defaultOnMissing.map(f)

  def emap[B](f: A => Either[String, B]): WireDecoder[B] =
    (reader: BsonReader) =>
      f(WireDecoder.this.decode(reader)) match
        case Right(value) => value
        case Left(reason) => throw BsonError.DecodingFailure(BsonError.InvalidValue(reason))

object WireDecoder:
  inline def apply[A](using instance: WireDecoder[A]): WireDecoder[A] = instance
