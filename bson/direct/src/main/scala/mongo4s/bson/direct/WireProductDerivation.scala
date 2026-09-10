package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.annotation.publicInBinary
import scala.compiletime.{constValueTuple, summonAll}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireProductDerivation:

  inline def derived[A](using m: Mirror.ProductOf[A], config: WireCodecConfig): WireCodec[A] =
    val declared: Array[String] = constValueTuple[m.MirroredElemLabels].toList.map(_.asInstanceOf[String]).toArray
    val labels: Array[String]   = declared.map(config.fieldNaming.apply)

    require(
      labels.distinct.length == labels.length,
      s"WireCodecConfig.fieldNaming produced duplicate field names: ${labels.mkString(", ")}",
    )

    make[A](
      mirror = m,
      labels = labels,
      declared = declared,
      naming = config.fieldNaming,
      omitAbsentFields = config.omitNoneFields,
      codecsThunk = () => summonAll[Tuple.Map[m.MirroredElemTypes, WireCodec]].toList.asInstanceOf[List[WireCodec[Any]]].toArray
    )
  end derived

  @publicInBinary private[direct] def make[A](
      mirror: Mirror.ProductOf[A],
      labels: Array[String],
      declared: Array[String],
      naming: mongo4s.bson.FieldNaming,
      omitAbsentFields: Boolean,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs = codecsThunk()
    val indexOf     = labels.zipWithIndex.toMap[String, Int]

    new FieldCodec[A]:
      override def fieldNaming: mongo4s.bson.FieldNaming = naming

      override def spellsAs(candidate: mongo4s.bson.FieldNaming): Boolean =
        declared.map(candidate.apply).sameElements(labels)
      override def fieldNames: Array[String]                              = labels
      override def isEmpty: Boolean                                       = labels.isEmpty

      override def readEmpty: A =
        if labels.isEmpty
        then mirror.fromProduct(EmptyTuple)
        else super.readEmpty

      def writeFields(writer: BsonWriter, value: A): Unit =
        val values = value.asInstanceOf[Product].productIterator
        var i      = 0
        while values.hasNext do
          val field = values.next()

          if !(omitAbsentFields && codecs(i).isAbsent(field))
          then
            writer.writeName(labels(i))
            codecs(i).encode(writer, field)

          i += 1
      end writeFields

      def readFields(reader: BsonReader): A =
        val values = new Array[Any](labels.length)

        while reader.readBsonType() != BsonType.END_OF_DOCUMENT
        do
          val name = reader.readName()
          indexOf.get(name) match
            case Some(idx) => values(idx) = codecs(idx).decode(reader)
            case None      => reader.skipValue()
        end while

        var i = 0

        while i < values.length
        do
          if values(i) == null
          then values(i) = codecs(i).defaultOnMissing.getOrElse(throw BsonError.DecodingFailure(BsonError.MissingField(labels(i))))
          i += 1
        end while

        mirror.fromProduct(Tuple.fromArray(values))
      end readFields
