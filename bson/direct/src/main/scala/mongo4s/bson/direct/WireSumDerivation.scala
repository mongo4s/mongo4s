package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireSumDerivation:

  private[direct] val DiscriminatorField = "_type"

  inline def derived[A](using m: Mirror.SumOf[A], config: WireCodecConfig): WireCodec[A] =
    val discriminators: Array[String] = labelsOf[m.MirroredElemLabels].toArray.map(config.discriminatorNaming.apply)
    require(
      discriminators.distinct.length == discriminators.length,
      s"WireCodecConfig.discriminatorNaming produced duplicate discriminators: ${discriminators.mkString(", ")}",
    )
    make[A](m, discriminators, config.encodeEmptyCasesAsString, () => codecsOf[m.MirroredElemTypes].toArray.asInstanceOf[Array[FieldCodec[Any]]])

  private def make[A](
      m: Mirror.SumOf[A],
      discriminators: Array[String],
      encodeEmptyCasesAsString: Boolean,
      codecsThunk: () => Array[FieldCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs: Array[FieldCodec[Any]] = codecsThunk()
    val indexOf: Map[String, Int]           = discriminators.zipWithIndex.toMap

    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit =
        val ordinal = m.ordinal(value)
        if encodeEmptyCasesAsString && codecs(ordinal).isEmpty then writer.writeString(discriminators(ordinal))
        else
          writer.writeStartDocument()
          writer.writeName(DiscriminatorField)
          writer.writeString(discriminators(ordinal))
          codecs(ordinal).writeFields(writer, value)
          writer.writeEndDocument()

      def decode(reader: BsonReader): A =
        if reader.getCurrentBsonType == BsonType.STRING then
          val tag = reader.readString()
          val idx = indexOf.getOrElse(tag, throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown subtype discriminator: $tag")))
          codecs(idx).readEmpty.asInstanceOf[A]
        else
          reader.readStartDocument()
          if reader.readBsonType() == BsonType.END_OF_DOCUMENT then throw BsonError.DecodingFailure(BsonError.MissingField(DiscriminatorField))

          val firstName = reader.readName()
          if firstName != DiscriminatorField then
            throw BsonError.DecodingFailure(
              BsonError.Custom(s"Expected discriminator field '$DiscriminatorField' first, got '$firstName'")
            )

          val tag    = reader.readString()
          val idx    = indexOf.getOrElse(tag, throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown subtype discriminator: $tag")))
          val result = codecs(idx).readFields(reader)
          reader.readEndDocument()
          result.asInstanceOf[A]

  private inline def labelsOf[T <: Tuple]: List[String] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => constValue[t].asInstanceOf[String] :: labelsOf[ts]

  private inline def codecsOf[T <: Tuple]: List[FieldCodec[?]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => summonInline[WireCodec[t]].asInstanceOf[FieldCodec[t]] :: codecsOf[ts]
