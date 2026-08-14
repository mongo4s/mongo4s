package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireSumDerivation:

  private[direct] val DiscriminatorField = "_type"

  inline def derived[A](using m: Mirror.SumOf[A]): WireCodec[A] =
    val discriminators: Array[String] = labelsOf[m.MirroredElemLabels].toArray
    make[A](m, discriminators, () => codecsOf[m.MirroredElemTypes].toArray.asInstanceOf[Array[FieldCodec[Any]]])

  // See WireProductDerivation.make for why `codecsThunk` is forced lazily rather than eagerly - a
  // sealed hierarchy nested inside itself (however indirectly, e.g. through a case's own List field)
  // would otherwise force this type's own not-yet-initialized `given` from inside its own
  // initializer and deadlock.
  private def make[A](m: Mirror.SumOf[A], discriminators: Array[String], codecsThunk: () => Array[FieldCodec[Any]]): WireCodec[A] =
    lazy val codecs: Array[FieldCodec[Any]] = codecsThunk()
    val indexOf: Map[String, Int]           = discriminators.zipWithIndex.toMap

    new WireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit =
        val ordinal = m.ordinal(value)
        writer.writeStartDocument()
        writer.writeName(DiscriminatorField)
        writer.writeString(discriminators(ordinal))
        codecs(ordinal).writeFields(writer, value)
        writer.writeEndDocument()

      def decode(reader: BsonReader): A =
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
