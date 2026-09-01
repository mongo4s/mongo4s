package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.bson.{BsonReader, BsonReaderMark, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireSumDerivation:

  private[direct] val DiscriminatorField = "_type"
  private[direct] val NestedValueField   = "value"

  inline def derived[A](using mirror: Mirror.SumOf[A], config: WireCodecConfig): WireCodec[A] =
    val discriminators: Array[String] = labelsOf[mirror.MirroredElemLabels].toArray.map(config.discriminatorNaming.apply)
    require(
      discriminators.distinct.length == discriminators.length,
      s"WireCodecConfig.discriminatorNaming produced duplicate discriminators: ${discriminators.mkString(", ")}",
    )
    make[A](
      mirror,
      discriminators,
      config.encodeEmptyCasesAsString,
      () => codecsOf[mirror.MirroredElemTypes].toArray.asInstanceOf[Array[WireCodec[Any]]],
    )

  private def make[A](
      mirror: Mirror.SumOf[A],
      discriminators: Array[String],
      encodeEmptyCasesAsString: Boolean,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs: Array[WireCodec[Any]] = codecsThunk()
    val indexOf: Map[String, Int]          = discriminators.zipWithIndex.toMap

    def unknown(tag: String): Nothing =
      throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown subtype discriminator: $tag"))

    def scanForDiscriminator(reader: BsonReader): String =
      var tag: String = null
      while tag == null && reader.readBsonType() != BsonType.END_OF_DOCUMENT do
        if reader.readName() == DiscriminatorField
        then tag = reader.readString()
        else reader.skipValue()

      if tag == null
      then throw BsonError.DecodingFailure(BsonError.MissingField(DiscriminatorField))
      else tag
    end scanForDiscriminator

    def rewindToDiscriminator(reader: BsonReader, start: BsonReaderMark): String =
      start.reset()
      val restart = reader.getMark()
      reader.readStartDocument()
      val tag     = scanForDiscriminator(reader)
      restart.reset()
      reader.readStartDocument()
      tag
    end rewindToDiscriminator

    def readNestedValue(reader: BsonReader, tag: String, nested: WireCodec[Any]): Any =
      var value: Any     = null
      var found: Boolean = false
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do
        val name = reader.readName()
        if !found && name == NestedValueField
        then
          value = nested.decode(reader)
          found = true
        else reader.skipValue()

      if !found
      then throw BsonError.DecodingFailure(BsonError.Custom(s"Expected '$NestedValueField' for subtype '$tag'"))
      else value
    end readNestedValue

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

      def decode(reader: BsonReader): A =
        if reader.getCurrentBsonType == BsonType.STRING then
          val tag = reader.readString()
          val idx = indexOf.getOrElse(tag, unknown(tag))

          codecs(idx) match
            case fields: FieldCodec[Any] => fields.readEmpty.asInstanceOf[A]
            case _                       =>
              throw BsonError.DecodingFailure(
                BsonError.Custom(s"Subtype '$tag' has a payload and cannot be read from a bare string")
              )
        else
          val start = reader.getMark()
          reader.readStartDocument()

          val firstName = if reader.readBsonType() == BsonType.END_OF_DOCUMENT then null else reader.readName()

          val tag =
            if firstName == DiscriminatorField
            then reader.readString()
            else rewindToDiscriminator(reader, start)

          val idx = indexOf.getOrElse(tag, unknown(tag))

          val result = codecs(idx) match
            case fields: FieldCodec[Any] => fields.readFields(reader)
            case nested                  => readNestedValue(reader, tag, nested)

          reader.readEndDocument()
          result.asInstanceOf[A]
        end if

  private inline def labelsOf[T <: Tuple]: List[String] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => constValue[t].asInstanceOf[String] :: labelsOf[ts]

  private inline def codecsOf[T <: Tuple]: List[WireCodec[?]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => summonInline[WireCodec[t]] :: codecsOf[ts]
