package mongo4s.bson.catsdata

import scala.collection.immutable.{SortedMap, SortedSet}

import cats.Order
import cats.data.{Chain, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}

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

object CatsDataWireInstances extends CatsDataWireInstances
