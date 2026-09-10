package mongo4s

import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValueTuple, summonAll}

import mongo4s.operations.Filter
import mongo4s.bson.{BsonEncoder, BsonField, FieldNaming}

trait PrimaryKey[E, K] extends KeyRef[E, K]:
  def key(entity: E): K

object PrimaryKey:
  inline def apply[E, K](using instance: PrimaryKey[E, K]): instance.type = instance

  def make[E, K](keyOf: E => K, names: List[String], fieldsOf: K => KeyFields): PrimaryKey[E, K] =
    new PrimaryKey[E, K]:
      def key(entity: E): K         = keyOf(entity)
      def fieldNames: List[String]  = names
      def fields(key: K): KeyFields = fieldsOf(key)

  def id[E, Id](keyOf: E => Id)(using encoder: BsonEncoder[Id]): PrimaryKey[E, Id] =
    single("id")(keyOf)

  def storedId[E, Id](keyOf: E => Id)(using encoder: BsonEncoder[Id]): PrimaryKey[E, Id] =
    single(BsonField.Id)(keyOf)

  def single[E, F1](name: String)(keyOf: E => F1)(using encoder: BsonEncoder[F1]): PrimaryKey[E, F1] =
    make(keyOf, List(name), key => KeyFields.one(name, encoder.encode(key)))

  inline def compound[E, N <: Tuple, V <: Tuple](
      inline keyOf: E => NamedTuple[N, V],
      naming: FieldNaming = FieldNaming.identity,
  ): PrimaryKey[E, NamedTuple[N, V]] =
    val names    = constValueTuple[N].toList.map(label => naming(label.asInstanceOf[String]))
    val encoders = summonAll[Tuple.Map[V, BsonEncoder]].toList.asInstanceOf[List[BsonEncoder[Any]]]

    require(
      names.distinct.length == names.length,
      s"a primary key cannot name the same field twice, got: ${names.mkString(", ")}",
    )

    val encodeEach = names.zip(encoders)

    make(
      keyOf,
      names,
      key =>
        val encoded = encodeEach.zip(key.toTuple.productIterator.toList).map { case ((name, encoder), value) =>
          name -> encoder.encode(value)
        }

        KeyFields(encoded.head, encoded.tail),
    )
  end compound

  extension [E, K](entity: E)(using pk: PrimaryKey[E, K]) def primaryKeyFilter: Filter[E] = pk.eqFilter(pk.key(entity))

  extension [E, K](entities: List[E])(using pk: PrimaryKey[E, K]) def primaryKeysFilter: Filter[E] = pk.inFilter(entities.map(pk.key))
