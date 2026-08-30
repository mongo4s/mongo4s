package mongo4s.bson.catsdata

import scala.reflect.ClassTag
import scala.collection.immutable.{SortedMap, SortedSet}

import cats.Order
import cats.data.{Chain, Ior, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError
import mongo4s.bson.direct.WireCodec

trait CatsDataWireInstances:

  private def nonEmpty[A](description: String): A =
    throw BsonError.DecodingFailure(BsonError.Custom(s"Expected a non-empty $description"))

  given [A](using inner: WireCodec[List[A]]): WireCodec[NonEmptyList[A]] =
    WireCodec.instance(
      (writer, value) => inner.encode(writer, value.toList),
      reader => NonEmptyList.fromList(inner.decode(reader)).getOrElse(nonEmpty("list")),
    )

  given [A](using inner: WireCodec[List[A]]): WireCodec[Chain[A]] =
    WireCodec.instance(
      (writer, value) => inner.encode(writer, value.toList),
      reader => Chain.fromSeq(inner.decode(reader)),
    )

  given [A](using inner: WireCodec[Vector[A]]): WireCodec[NonEmptyVector[A]] =
    WireCodec.instance(
      (writer, value) => inner.encode(writer, value.toVector),
      reader => NonEmptyVector.fromVector(inner.decode(reader)).getOrElse(nonEmpty("vector")),
    )

  given [A](using inner: WireCodec[Set[A]], order: Order[A]): WireCodec[NonEmptySet[A]] =
    WireCodec.instance(
      (writer, value) => inner.encode(writer, value.toSortedSet),
      reader => NonEmptySet.fromSet(SortedSet.from(inner.decode(reader))(using order.toOrdering)).getOrElse(nonEmpty("set")),
    )

  given [A](using inner: WireCodec[Map[String, A]]): WireCodec[NonEmptyMap[String, A]] =
    WireCodec.instance(
      (writer, value) => inner.encode(writer, value.toSortedMap),
      reader => NonEmptyMap.fromMap(SortedMap.from(inner.decode(reader))).getOrElse(nonEmpty("map")),
    )

  private val IorDiscriminatorField = "_type"

  given iorWireCodec[A, B](using
      codecA: WireCodec[A],
      codecB: WireCodec[B],
      tagA: ClassTag[A],
      tagB: ClassTag[B],
  ): WireCodec[Ior[A, B]] =
    val nameA   = tagA.runtimeClass.getSimpleName
    val nameB   = tagB.runtimeClass.getSimpleName
    require(
      nameA != nameB,
      s"Ior's two type parameters must have distinguishable type names to derive a WireCodec — both were '$nameA'",
    )
    val bothTag = s"$nameA+$nameB"

    def writeSingle[T](writer: BsonWriter, tag: String, value: T, codec: WireCodec[T]): Unit =
      writer.writeStartDocument()
      writer.writeName(IorDiscriminatorField)
      writer.writeString(tag)
      writer.writeName("value")
      codec.encode(writer, value)
      writer.writeEndDocument()

    def readTag(reader: BsonReader): String =
      reader.readStartDocument()
      if reader.readBsonType() == BsonType.END_OF_DOCUMENT then throw BsonError.DecodingFailure(BsonError.MissingField(IorDiscriminatorField))
      val firstName = reader.readName()
      if firstName != IorDiscriminatorField then
        throw BsonError.DecodingFailure(BsonError.Custom(s"Expected discriminator field '$IorDiscriminatorField' first, got '$firstName'"))
      reader.readString()

    WireCodec.instance(
      (writer, value) =>
        value match
          case Ior.Left(a)    => writeSingle(writer, nameA, a, codecA)
          case Ior.Right(b)   => writeSingle(writer, nameB, b, codecB)
          case Ior.Both(a, b) =>
            writer.writeStartDocument()
            writer.writeName(IorDiscriminatorField)
            writer.writeString(bothTag)
            writer.writeName("left")
            codecA.encode(writer, a)
            writer.writeName("right")
            codecB.encode(writer, b)
            writer.writeEndDocument(),
      reader =>
        val tag               = readTag(reader)
        val result: Ior[A, B] =
          if tag == nameA then
            reader.readName()
            Ior.Left(codecA.decode(reader))
          else if tag == nameB then
            reader.readName()
            Ior.Right(codecB.decode(reader))
          else if tag == bothTag then
            reader.readName()
            val a = codecA.decode(reader)
            reader.readName()
            val b = codecB.decode(reader)
            Ior.Both(a, b)
          else throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown Ior discriminator: $tag"))
        reader.readEndDocument()
        result,
    )

object CatsDataWireInstances extends CatsDataWireInstances
