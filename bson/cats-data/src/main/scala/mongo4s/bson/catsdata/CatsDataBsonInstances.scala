package mongo4s.bson.catsdata

import scala.collection.immutable.{SortedMap, SortedSet}

import cats.Order
import cats.data.{Chain, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError}

trait CatsDataBsonInstances:

  given [A](using enc: BsonEncoder[List[A]]): BsonEncoder[NonEmptyList[A]] =
    enc.contramap(_.toList)
  given [A](using dec: BsonDecoder[List[A]]): BsonDecoder[NonEmptyList[A]] =
    dec.emap(list => NonEmptyList.fromList(list).toRight(BsonError.Custom("Expected a non-empty list")))

  given [A](using enc: BsonEncoder[List[A]]): BsonEncoder[Chain[A]] = enc.contramap(_.toList)
  given [A](using dec: BsonDecoder[List[A]]): BsonDecoder[Chain[A]] = dec.map(Chain.fromSeq)

  given [A](using enc: BsonEncoder[Vector[A]]): BsonEncoder[NonEmptyVector[A]] =
    enc.contramap(_.toVector)
  given [A](using dec: BsonDecoder[Vector[A]]): BsonDecoder[NonEmptyVector[A]] =
    dec.emap(vector => NonEmptyVector.fromVector(vector).toRight(BsonError.Custom("Expected a non-empty vector")))

  given [A](using enc: BsonEncoder[Set[A]]): BsonEncoder[NonEmptySet[A]]                  =
    enc.contramap(_.toSortedSet)
  given [A](using dec: BsonDecoder[Set[A]], order: Order[A]): BsonDecoder[NonEmptySet[A]] =
    dec.emap: set =>
      NonEmptySet.fromSet(SortedSet.from(set)(using order.toOrdering)).toRight(BsonError.Custom("Expected a non-empty set"))

  given [A](using enc: BsonEncoder[Map[String, A]]): BsonEncoder[NonEmptyMap[String, A]] =
    enc.contramap(_.toSortedMap)
  given [A](using dec: BsonDecoder[Map[String, A]]): BsonDecoder[NonEmptyMap[String, A]] =
    dec.emap(map => NonEmptyMap.fromMap(SortedMap.from(map)).toRight(BsonError.Custom("Expected a non-empty map")))

object CatsDataBsonInstances extends CatsDataBsonInstances
