package mongo4s.bson.direct

import org.bson.BsonWriter

trait WireEncoder[A]:
  def encode(writer: BsonWriter, value: A): Unit

  def contramap[B](f: B => A): WireEncoder[B] =
    (writer, value) => encode(writer, f(value))

object WireEncoder:
  inline def apply[A](using instance: WireEncoder[A]): WireEncoder[A] = instance
