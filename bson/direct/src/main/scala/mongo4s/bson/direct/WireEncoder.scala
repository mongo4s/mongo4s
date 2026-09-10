package mongo4s.bson.direct

import org.bson.BsonWriter

@annotation.implicitNotFound(
  "No WireEncoder[${A}]. A WireCodec[${A}] supplies one; otherwise give the type `derives WireCodec`, "
    + "or write the encode side by hand."
)
trait WireEncoder[A]:
  def encode(writer: BsonWriter, value: A): Unit

  def isAbsent(value: A): Boolean = false

  def contramap[B](f: B => A): WireEncoder[B] =
    new WireEncoder[B]:
      def encode(writer: BsonWriter, value: B): Unit = WireEncoder.this.encode(writer, f(value))
      override def isAbsent(value: B): Boolean       = WireEncoder.this.isAbsent(f(value))

object WireEncoder:
  inline def apply[A](using instance: WireEncoder[A]): WireEncoder[A] = instance
