package mongo4s.bson.direct

import org.bson.BsonReader

import mongo4s.bson.BsonError

@annotation.implicitNotFound(
  "No WireDecoder[${A}]. A WireCodec[${A}] supplies one; otherwise give the type `derives WireCodec`, "
    + "or write the decode side by hand."
)
trait WireDecoder[A]:
  def decode(reader: BsonReader): A

  def defaultOnMissing: Option[A] = None

  def map[B](f: A => B): WireDecoder[B] =
    new WireDecoder[B]:
      def decode(reader: BsonReader): B        = f(WireDecoder.this.decode(reader))
      override def defaultOnMissing: Option[B] = WireDecoder.this.defaultOnMissing.map(f)

  def emap[B](f: A => Either[String, B]): WireDecoder[B] =
    new WireDecoder[B]:
      def decode(reader: BsonReader): B        = WireDecoder.orFail(f)(WireDecoder.this.decode(reader))
      override def defaultOnMissing: Option[B] = WireDecoder.this.defaultOnMissing.flatMap(f(_).toOption)

object WireDecoder:
  private[direct] def orFail[A, B](f: A => Either[String, B]): A => B =
    value =>
      f(value) match
        case Right(result) => result
        case Left(reason)  => throw BsonError.DecodingFailure(BsonError.InvalidValue(reason))

  inline def apply[A](using instance: WireDecoder[A]): WireDecoder[A] = instance
