package mongo4s.bson.direct

import org.bson.{BsonReader, BsonType, BsonWriter}
import org.bson.codecs.{BsonValueCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError}

trait WireFallbackInstances extends WireCodecLowPriorityDerivation:

  given keyedMapWireCodec: [K, V] => (keys: KeyCodec[K], inner: WireCodec[V]) => WireCodec[Map[K, V]]:
    def encode(writer: BsonWriter, values: Map[K, V]): Unit =
      writer.writeStartDocument()
      values.foreach { (key, value) =>
        writer.writeName(keys.encodeKey(key))
        inner.encode(writer, value)
      }
      writer.writeEndDocument()
    end encode

    def decode(reader: BsonReader): Map[K, V] =
      reader.readStartDocument()
      val builder = Map.newBuilder[K, V]

      while reader.readBsonType() != BsonType.END_OF_DOCUMENT
      do
        val raw = reader.readName()
        val key = keys.decodeKey(raw).fold(reason => throw BsonError.DecodingFailure(BsonError.Nested(raw, BsonError.InvalidValue(reason))), identity)
        builder.addOne((key, WirePath.under(raw)(inner.decode(reader))))
      end while

      reader.readEndDocument()
      builder.result()
    end decode
  end keyedMapWireCodec

  given fromBsonValueCodec: [A] => (encoder: BsonEncoder[A], decoder: BsonDecoder[A]) => WireCodec[A] =
    new WireCodec[A]:
      private val bsonValueCodec = BsonValueCodec()

      def encode(writer: BsonWriter, value: A): Unit =
        bsonValueCodec.encode(writer, encoder.encode(value), EncoderContext.builder().build())

      def decode(reader: BsonReader): A =
        decoder.decode(bsonValueCodec.decode(reader, DecoderContext.builder().build())) match
          case Right(value) => value
          case Left(error)  => throw error.toThrowable
