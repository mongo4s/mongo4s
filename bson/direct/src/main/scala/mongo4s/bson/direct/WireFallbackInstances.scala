package mongo4s.bson.direct

import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{BsonValueCodec, DecoderContext, EncoderContext}

import mongo4s.bson.{BsonDecoder, BsonEncoder}

trait WireFallbackInstances extends WireCodecLowPriorityDerivation:

  given fromBsonValueCodec[A](using encoder: BsonEncoder[A], decoder: BsonDecoder[A]): WireCodec[A] =
    new WireCodec[A]:
      private val bsonValueCodec = BsonValueCodec()

      def encode(writer: BsonWriter, value: A): Unit =
        bsonValueCodec.encode(writer, encoder.encode(value), EncoderContext.builder().build())

      def decode(reader: BsonReader): A =
        decoder.decode(bsonValueCodec.decode(reader, DecoderContext.builder().build())) match
          case Right(value) => value
          case Left(error)  => throw error.toThrowable
