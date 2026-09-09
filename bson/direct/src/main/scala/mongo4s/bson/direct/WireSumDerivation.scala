package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.annotation.publicInBinary
import scala.compiletime.{constValueTuple, summonAll}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireSumDerivation:

  private[direct] val DiscriminatorField = WireDiscriminator.Field
  private[direct] val NestedValueField   = WireDiscriminator.ValueField

  inline def derived[A](using mirror: Mirror.SumOf[A], config: WireCodecConfig): WireCodec[A] =
    val discriminators: Array[String] = constValueTuple[mirror.MirroredElemLabels].toList.map(label => config.discriminatorNaming(label.asInstanceOf[String])).toArray
    require(
      discriminators.distinct.length == discriminators.length,
      s"WireCodecConfig.discriminatorNaming produced duplicate discriminators: ${discriminators.mkString(", ")}",
    )
    make[A](
      mirror,
      discriminators,
      config.encodeEmptyCasesAsString,
      () => summonAll[Tuple.Map[mirror.MirroredElemTypes, WireCodec]].toList.asInstanceOf[List[WireCodec[Any]]].toArray,
    )
  end derived

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.SumOf[A],
      discriminators: Array[String],
      encodeEmptyCasesAsString: Boolean,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs: Array[WireCodec[Any]] =
      val built = codecsThunk()
      var i     = 0

      while i < built.length
      do
        built(i) match
          case fields: FieldCodec[Any] =>
            require(
              !fields.fieldNames.contains(DiscriminatorField),
              s"subtype '${discriminators(i)}' declares a field named '$DiscriminatorField', which collides with the sum discriminator",
            )
          case _                       => ()
        i += 1
      built
    end codecs

    val indexOf = discriminators.zipWithIndex.toMap[String, Int]

    def unknown(tag: String): Nothing =
      throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown subtype discriminator: $tag"))

    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit =
        val ordinal = mirror.ordinal(value)

        codecs(ordinal) match
          case fields: FieldCodec[Any] if encodeEmptyCasesAsString && fields.isEmpty =>
            writer.writeString(discriminators(ordinal))

          case fields: FieldCodec[Any] =>
            writer.writeStartDocument()
            writer.writeName(DiscriminatorField)
            writer.writeString(discriminators(ordinal))
            fields.writeFields(writer, value)
            writer.writeEndDocument()

          case nested =>
            writer.writeStartDocument()
            writer.writeName(DiscriminatorField)
            writer.writeString(discriminators(ordinal))
            writer.writeName(NestedValueField)
            nested.encode(writer, value)
            writer.writeEndDocument()
      end encode

      def decode(reader: BsonReader): A =
        if reader.getCurrentBsonType == BsonType.STRING
        then
          val tag = reader.readString()
          val idx = indexOf.getOrElse(tag, unknown(tag))

          codecs(idx) match
            case fields: FieldCodec[Any] if fields.isEmpty => fields.readEmpty.asInstanceOf[A]
            case _                                         =>
              throw BsonError.DecodingFailure(
                BsonError.Custom(s"Subtype '$tag' has a payload and cannot be read from a bare string")
              )
        else
          val tag = WireDiscriminator.read(reader)
          val idx = indexOf.getOrElse(tag, unknown(tag))

          val result = codecs(idx) match
            case fields: FieldCodec[Any] => fields.readFields(reader)
            case nested                  => WireDiscriminator.readValue(reader, nested)

          reader.readEndDocument()
          result.asInstanceOf[A]
      end decode
