package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.annotation.publicInBinary
import scala.compiletime.constValueTuple

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireProductDerivation:

  private val Unread: AnyRef = new Object

  inline def derived[A](using m: Mirror.ProductOf[A], config: WireCodecConfig): WireCodec[A] =
    build[A](
      mirror = m,
      names = constValueTuple[m.MirroredElemLabels].toArray.map(_.asInstanceOf[String]),
      storedNames = WireFields.storedNames[A],
      notStored = WireFields.notStored[A],
      config = config,
      defaults = WireDefaults.of[A],
      codecsThunk = WireFields.codecs[A],
    )

  @publicInBinary private[direct] def build[A](
      mirror: Mirror.ProductOf[A],
      names: Array[String],
      storedNames: Array[String],
      notStored: Array[Boolean],
      config: WireCodecConfig,
      defaults: Map[Int, () => Any],
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    val labels  = names.indices.map(i => if storedNames(i) != null then storedNames(i) else config.fieldNaming(names(i))).toArray
    val written = labels.indices.filterNot(notStored).map(labels)

    require(
      written.distinct.length == written.length,
      s"WireCodecConfig.fieldNaming and @stored produced duplicate field names: ${written.mkString(", ")}",
    )

    make[A](
      mirror = mirror,
      labels = labels,
      naming = config.fieldNaming,
      omitAbsentFields = config.omitNoneFields,
      defaults = if config.decodeMissingAsDefault then defaults else defaults.filter((index, _) => notStored(index)),
      notStored = notStored,
      codecsThunk = codecsThunk,
    )
  end build

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.ProductOf[A],
      labels: Array[String],
      naming: mongo4s.bson.FieldNaming,
      omitAbsentFields: Boolean,
      defaults: Map[Int, () => Any],
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    make(mirror, labels, naming, omitAbsentFields, defaults, new Array[Boolean](labels.length), codecsThunk)

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.ProductOf[A],
      labels: Array[String],
      naming: mongo4s.bson.FieldNaming,
      omitAbsentFields: Boolean,
      defaults: Map[Int, () => Any],
      notStored: Array[Boolean],
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs = codecsThunk()
    val stored      = labels.indices.filterNot(notStored)
    val indexOf     = stored.map(i => (labels(i), i)).toMap
    val storedNames = stored.map(labels).toArray

    new FieldCodec[A]:
      override def fieldNaming: mongo4s.bson.FieldNaming = naming
      override def fieldNames: Array[String]             = storedNames
      override def isEmpty: Boolean                      = labels.isEmpty

      override def readEmpty: A =
        if labels.isEmpty
        then mirror.fromProduct(EmptyTuple)
        else super.readEmpty

      def writeFields(writer: BsonWriter, value: A): Unit =
        val product     = value.asInstanceOf[Product]
        val fieldCodecs = codecs
        var i           = 0

        while i < labels.length
        do
          val field = product.productElement(i)

          if !notStored(i) && !(omitAbsentFields && fieldCodecs(i).isAbsent(field))
          then
            writer.writeName(labels(i))
            fieldCodecs(i).encode(writer, field)

          i += 1
        end while
      end writeFields

      def readFields(reader: BsonReader): A =
        val values = new Array[Any](labels.length)
        java.util.Arrays.fill(values.asInstanceOf[Array[AnyRef]], WireProductDerivation.Unread)

        while reader.readBsonType() != BsonType.END_OF_DOCUMENT
        do
          val name = reader.readName()
          indexOf.get(name) match
            case Some(idx) => values(idx) = WirePath.under(labels(idx))(codecs(idx).decode(reader))
            case None      => reader.skipValue()
        end while

        var i = 0

        while i < values.length
        do
          if values(i).asInstanceOf[AnyRef] eq WireProductDerivation.Unread
          then
            values(i) = defaults.get(i) match
              case Some(default) => default()
              case None          => codecs(i).defaultOnMissing.getOrElse(throw BsonError.DecodingFailure(BsonError.MissingField(labels(i))))
          i += 1
        end while

        mirror.fromProduct(Tuple.fromArray(values))
      end readFields
