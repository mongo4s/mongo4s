package mongo4s.bson.catsdata

import scala.reflect.ClassTag
import scala.collection.immutable.{SortedMap, SortedSet}

import cats.Order
import cats.data.{Chain, Ior, NonEmptyList, NonEmptyMap, NonEmptySet, NonEmptyVector}
import org.bson.{BsonDocument, BsonString, BsonValue}

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError, BsonTypeName}

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

  private val IorDiscriminatorField = "_type"

  private def iorNames[A, B](using tagA: ClassTag[A], tagB: ClassTag[B]): (String, String, String) =
    val nameA = tagA.runtimeClass.getSimpleName
    val nameB = tagB.runtimeClass.getSimpleName
    require(
      nameA != nameB,
      s"Ior's two type parameters must have distinguishable type names to derive a BsonEncoder/BsonDecoder — both were '$nameA'",
    )
    (nameA, nameB, s"$nameA+$nameB")

  given [A, B](using encA: BsonEncoder[A], encB: BsonEncoder[B], tagA: ClassTag[A], tagB: ClassTag[B]): BsonEncoder[Ior[A, B]] =
    val (nameA, nameB, bothTag) = iorNames[A, B]
    {
      case Ior.Left(a)    => BsonDocument(IorDiscriminatorField, BsonString(nameA)).append("value", encA.encode(a))
      case Ior.Right(b)   => BsonDocument(IorDiscriminatorField, BsonString(nameB)).append("value", encB.encode(b))
      case Ior.Both(a, b) =>
        BsonDocument(IorDiscriminatorField, BsonString(bothTag)).append("left", encA.encode(a)).append("right", encB.encode(b))
    }

  given [A, B](using decA: BsonDecoder[A], decB: BsonDecoder[B], tagA: ClassTag[A], tagB: ClassTag[B]): BsonDecoder[Ior[A, B]] =
    val (nameA, nameB, bothTag) = iorNames[A, B]

    def field(doc: BsonDocument, key: String): Either[BsonError, BsonValue] =
      if doc.containsKey(key) then Right(doc.get(key)) else Left(BsonError.MissingField(key))

    (bson: BsonValue) =>
      if !bson.isDocument then Left(BsonError.typeMismatch(BsonTypeName.Object, bson))
      else
        val doc = bson.asDocument
        field(doc, IorDiscriminatorField).flatMap(_.asString.getValue match
          case tag if tag == nameA   => field(doc, "value").flatMap(decA.decode).map(Ior.Left(_))
          case tag if tag == nameB   => field(doc, "value").flatMap(decB.decode).map(Ior.Right(_))
          case tag if tag == bothTag =>
            for
              left  <- field(doc, "left").flatMap(decA.decode)
              right <- field(doc, "right").flatMap(decB.decode)
            yield Ior.Both(left, right)
          case other                 => Left(BsonError.Custom(s"Unknown Ior discriminator: $other")),
        )

object CatsDataBsonInstances extends CatsDataBsonInstances
