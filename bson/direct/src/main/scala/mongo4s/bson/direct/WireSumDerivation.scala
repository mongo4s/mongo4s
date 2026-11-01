package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type, Varargs}
import scala.annotation.publicInBinary
import scala.compiletime.constValueTuple

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.{BsonError, BsonField}

object WireSumDerivation:

  private[direct] val DiscriminatorField = WireDiscriminator.Field
  private[direct] val NestedValueField   = WireDiscriminator.ValueField

  private val Unread: AnyRef = new Object

  inline def derived[A](using mirror: Mirror.SumOf[A], config: WireCodecConfig): WireCodec[A] =
    val discriminators: Array[String] = constValueTuple[mirror.MirroredElemLabels].toList.map(label => config.discriminatorNaming(label.asInstanceOf[String])).toArray
    require(
      discriminators.distinct.length == discriminators.length,
      s"WireCodecConfig.discriminatorNaming produced duplicate discriminators: ${discriminators.mkString(", ")}",
    )
    make[A](mirror, discriminators, config, childCodecs[A](mirror, config))
  end derived

  private inline def childCodecs[A](mirror: Mirror.SumOf[A], config: WireCodecConfig): () => Array[WireCodec[Any]] =
    ${ childCodecsOf[A]('mirror, 'config) }

  @publicInBinary private[direct] def childCodecsOf[A: Type](mirror: Expr[Mirror.SumOf[A]], config: Expr[WireCodecConfig])(using
      quotes: Quotes
  ): Expr[() => Array[WireCodec[Any]]] =
    import quotes.reflect.*

    def codecOf[T: Type]: Expr[WireCodec[Any]] =
      Expr.summon[WireCodec[T]] match
        case Some(codec) => '{ $codec.asInstanceOf[WireCodec[Any]] }
        case None        =>
          Expr.summon[Mirror.Of[T]] match
            case Some(childMirror) => '{ WireCodec.derived[T](using $childMirror, $config).asInstanceOf[WireCodec[Any]] }
            case None              => report.errorAndAbort(s"No WireCodec[${Type.show[T]}] for a case of ${Type.show[A]}, and no Mirror to derive one from")

    def cases(elements: Type[?]): List[Expr[WireCodec[Any]]] =
      elements match
        case '[EmptyTuple]   => Nil
        case '[head *: tail] => codecOf[head] :: cases(Type.of[tail])
        case _               => report.errorAndAbort(s"Cannot list the cases of ${Type.show[A]}")

    mirror match
      case '{ $_ : Mirror.SumOf[A] { type MirroredElemTypes = elements } } =>
        val built = cases(Type.of[elements])
        '{ () => Array[WireCodec[Any]](${ Varargs(built) }*) }
      case _                                                               =>
        report.errorAndAbort(s"Cannot read the cases of ${Type.show[A]} from its Mirror")
  end childCodecsOf

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.SumOf[A],
      discriminators: Array[String],
      naming: mongo4s.bson.FieldNaming,
      encodeEmptyCasesAsString: Boolean,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    make(
      mirror,
      discriminators,
      WireCodecConfig.Default.withFieldNaming(naming).withEncodeEmptyCasesAsString(encodeEmptyCasesAsString),
      codecsThunk,
    )

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.SumOf[A],
      discriminators: Array[String],
      config: WireCodecConfig,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    val field   = config.discriminatorField
    val wrapped = config.wrappedCases
    val indexOf = discriminators.zipWithIndex.toMap[String, Int]
    val spare   = config.fallbackCase.flatMap(indexOf.get).getOrElse(-1)

    lazy val codecs: Array[WireCodec[Any]] =
      val built = codecsThunk()
      var i     = 0

      while i < built.length
      do
        built(i) match
          case fields: FieldCodec[Any] =>
            require(
              wrapped || !fields.fieldNames.contains(field),
              s"subtype '${discriminators(i)}' declares a field named '$field', which collides with the sum discriminator",
            )
          case _                       => ()
        i += 1

      if spare >= 0
      then
        built(spare) match
          case fields: FieldCodec[Any] if fields.isEmpty => ()
          case _                                         =>
            throw IllegalArgumentException(
              s"the fallback case '${discriminators(spare)}' carries fields, so an unknown subtype cannot be read as it — make it a case object"
            )

      built
    end codecs

    def caseOf(tag: String): Int =
      indexOf.getOrElse(
        tag,
        if spare >= 0 then -1
        else throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown subtype discriminator: $tag")),
      )

    def fallback: A = codecs(spare).asInstanceOf[FieldCodec[Any]].readEmpty.asInstanceOf[A]

    new WireCodec[A]:
      override def fieldNaming: mongo4s.bson.FieldNaming = config.fieldNaming

      def encode(writer: BsonWriter, value: A): Unit =
        val ordinal = mirror.ordinal(value)

        codecs(ordinal) match
          case fields: FieldCodec[Any] if config.encodeEmptyCasesAsString && fields.isEmpty =>
            writer.writeString(discriminators(ordinal))

          case child if wrapped =>
            writer.writeStartDocument()
            writer.writeName(discriminators(ordinal))
            child.encode(writer, value)
            writer.writeEndDocument()

          case fields: FieldCodec[Any] =>
            writer.writeStartDocument()
            writer.writeName(field)
            writer.writeString(discriminators(ordinal))
            fields.writeFields(writer, value)
            writer.writeEndDocument()

          case nested =>
            writer.writeStartDocument()
            writer.writeName(field)
            writer.writeString(discriminators(ordinal))
            writer.writeName(NestedValueField)
            nested.encode(writer, value)
            writer.writeEndDocument()
      end encode

      def decode(reader: BsonReader): A =
        if reader.getCurrentBsonType == BsonType.STRING
        then
          val tag = reader.readString()
          val idx = caseOf(tag)

          if idx < 0
          then fallback
          else
            codecs(idx) match
              case fields: FieldCodec[Any] if fields.isEmpty => fields.readEmpty.asInstanceOf[A]
              case _                                         =>
                throw BsonError.DecodingFailure(
                  BsonError.Custom(s"Subtype '$tag' has a payload and cannot be read from a bare string")
                )
        else if wrapped
        then decodeWrapped(reader)
        else
          val tag = WireDiscriminator.open(reader, field)
          val idx = caseOf(tag)

          val result =
            if idx < 0
            then
              WireDiscriminator.skipRest(reader)
              fallback
            else
              codecs(idx) match
                case fields: FieldCodec[Any] => fields.readFields(reader)
                case nested                  => WireDiscriminator.readValue(reader, nested)

          reader.readEndDocument()
          result.asInstanceOf[A]
      end decode

      private def decodeWrapped(reader: BsonReader): A =
        reader.readStartDocument()

        var result: Any = WireSumDerivation.Unread
        while (result.asInstanceOf[AnyRef] eq WireSumDerivation.Unread) && reader.readBsonType() != BsonType.END_OF_DOCUMENT
        do
          val name = reader.readName()

          if name == BsonField.Id
          then reader.skipValue()
          else
            val idx = caseOf(name)

            result =
              if idx < 0
              then
                reader.skipValue()
                fallback
              else WirePath.under(name)(codecs(idx).decode(reader))
        end while

        if result.asInstanceOf[AnyRef] eq WireSumDerivation.Unread
        then throw BsonError.DecodingFailure(BsonError.Custom("A wrapped case needs a field naming the case, and the document has none"))

        WireDiscriminator.skipRest(reader)
        reader.readEndDocument()
        result.asInstanceOf[A]
      end decodeWrapped
