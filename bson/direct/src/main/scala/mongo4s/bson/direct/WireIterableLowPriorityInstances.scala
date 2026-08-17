package mongo4s.bson.direct

import scala.collection.Factory

import org.bson.{BsonReader, BsonType, BsonWriter}

private[direct] trait WireIterableLowPriorityInstances:
  given iterableWireCodec[A, C[X] <: Iterable[X]](using inner: WireCodec[A], factory: Factory[A, C[A]]): WireCodec[C[A]] with
    def encode(writer: BsonWriter, values: C[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): C[A] =
      reader.readStartArray()
      val builder = factory.newBuilder
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()
