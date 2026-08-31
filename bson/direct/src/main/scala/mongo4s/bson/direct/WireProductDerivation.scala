package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireProductDerivation:

  inline def derived[A](using m: Mirror.ProductOf[A], config: WireCodecConfig): WireCodec[A] =
    val labels: Array[String] = labelsOf[m.MirroredElemLabels].toArray.map(config.fieldNaming.apply)
    require(labels.distinct.length == labels.length, s"WireCodecConfig.fieldNaming produced duplicate field names: ${labels.mkString(", ")}")
    make[A](m, labels, config.omitNoneFields, () => codecsOf[m.MirroredElemTypes].toArray.asInstanceOf[Array[WireCodec[Any]]])

  private def make[A](
      m: Mirror.ProductOf[A],
      labels: Array[String],
      omitAbsentFields: Boolean,
      codecsThunk: () => Array[WireCodec[Any]],
  ): WireCodec[A] =
    lazy val codecs: Array[WireCodec[Any]] = codecsThunk()
    val indexOf: Map[String, Int]          = labels.zipWithIndex.toMap

    new FieldCodec[A]:
      override def isEmpty: Boolean = labels.isEmpty
      override def readEmpty: A     = m.fromProduct(EmptyTuple)

      def writeFields(writer: BsonWriter, value: A): Unit =
        val values = value.asInstanceOf[Product].productIterator
        var i      = 0
        while values.hasNext do
          val field = values.next()
          if !(omitAbsentFields && codecs(i).isAbsent(field)) then
            writer.writeName(labels(i))
            codecs(i).encode(writer, field)
          i += 1

      def readFields(reader: BsonReader): A =
        val values = new Array[Any](labels.length)
        while reader.readBsonType() != BsonType.END_OF_DOCUMENT do
          val name = reader.readName()
          indexOf.get(name) match
            case Some(idx) => values(idx) = codecs(idx).decode(reader)
            case None      => reader.skipValue()

        var i = 0
        while i < values.length do
          if values(i) == null then values(i) = codecs(i).defaultOnMissing.getOrElse(throw BsonError.DecodingFailure(BsonError.MissingField(labels(i))))
          i += 1

        m.fromProduct(Tuple.fromArray(values))

  private inline def labelsOf[T <: Tuple]: List[String] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => constValue[t].asInstanceOf[String] :: labelsOf[ts]

  private inline def codecsOf[T <: Tuple]: List[WireCodec[?]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  => summonInline[WireCodec[t]] :: codecsOf[ts]
