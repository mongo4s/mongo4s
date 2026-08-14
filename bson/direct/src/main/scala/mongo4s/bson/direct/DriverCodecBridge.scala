package mongo4s.bson.direct

import scala.reflect.ClassTag

import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

object DriverCodecBridge:
  def toDriverCodec[A](using codec: WireCodec[A], tag: ClassTag[A]): Codec[A] =
    new Codec[A]:
      def encode(writer: BsonWriter, value: A, encoderContext: EncoderContext): Unit = codec.encode(writer, value)
      def decode(reader: BsonReader, decoderContext: DecoderContext): A              = codec.decode(reader)
      def getEncoderClass: Class[A]                                                  = tag.runtimeClass.asInstanceOf[Class[A]]
