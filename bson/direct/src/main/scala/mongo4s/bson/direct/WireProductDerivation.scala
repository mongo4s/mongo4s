package mongo4s.bson.direct

import scala.deriving.Mirror
import scala.compiletime.{constValue, erasedValue, summonInline}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

object WireProductDerivation:

  inline def derived[A](using m: Mirror.ProductOf[A]): WireCodec[A] =
    val labels: Array[String] = labelsOf[m.MirroredElemLabels].toArray
    make[A](m, labels, () => codecsOf[m.MirroredElemTypes].toArray.asInstanceOf[Array[WireCodec[Any]]])

  // `codecsThunk` is forced lazily, on first actual encode/decode call, not while this codec is
  // itself being constructed. A self-referential type (e.g. `case class Tree(children: List[Tree])
  // derives WireCodec`) needs `WireCodec[Tree]` to build its own `List[Tree]` field codec - summoning
  // that eagerly here would force `Tree`'s own not-yet-initialized `given` from inside its own
  // initializer and deadlock on the lazy-val's `CountDownLatch` (the same failure mode as a
  // trait-wide `given Effect[F]` self-reference - see RepositoryBackendSpec's `repo` method for that
  // one). Deferring to first use means `Tree`'s `given` has already finished initializing by the time
  // anything actually needs it.
  private def make[A](m: Mirror.ProductOf[A], labels: Array[String], codecsThunk: () => Array[WireCodec[Any]]): WireCodec[A] =
    lazy val codecs: Array[WireCodec[Any]] = codecsThunk()
    val indexOf: Map[String, Int]          = labels.zipWithIndex.toMap

    new FieldCodec[A]:
      def writeFields(writer: BsonWriter, value: A): Unit =
        val values = value.asInstanceOf[Product].productIterator
        var i      = 0
        while values.hasNext do
          writer.writeName(labels(i))
          codecs(i).encode(writer, values.next())
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
