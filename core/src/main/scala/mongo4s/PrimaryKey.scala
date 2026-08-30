package mongo4s

import mongo4s.bson.BsonEncoder
import mongo4s.operations.Filter

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
    single("_id")(keyOf)

  def single[E, F1](name: String)(keyOf: E => F1)(using encoder: BsonEncoder[F1]): PrimaryKey[E, F1] =
    make(keyOf, List(name), key => KeyFields.one(name, encoder.encode(key)))

  def compound[E, K, F1, F2](keyOf: E => K)(name1: String, value1: K => F1)(name2: String, value2: K => F2)(using
      e1: BsonEncoder[F1],
      e2: BsonEncoder[F2],
  ): PrimaryKey[E, K] =
    make(
      keyOf,
      List(name1, name2),
      key => KeyFields.of(name1 -> e1.encode(value1(key)), name2 -> e2.encode(value2(key))),
    )

  def compound3[E, K, F1, F2, F3](keyOf: E => K)(name1: String, value1: K => F1)(name2: String, value2: K => F2)(
      name3: String,
      value3: K => F3,
  )(using e1: BsonEncoder[F1], e2: BsonEncoder[F2], e3: BsonEncoder[F3]): PrimaryKey[E, K] =
    make(
      keyOf,
      List(name1, name2, name3),
      key => KeyFields.of(name1 -> e1.encode(value1(key)), name2 -> e2.encode(value2(key)), name3 -> e3.encode(value3(key))),
    )

  def compound4[E, K, F1, F2, F3, F4](keyOf: E => K)(name1: String, value1: K => F1)(name2: String, value2: K => F2)(
      name3: String,
      value3: K => F3,
  )(name4: String, value4: K => F4)(using
      e1: BsonEncoder[F1],
      e2: BsonEncoder[F2],
      e3: BsonEncoder[F3],
      e4: BsonEncoder[F4],
  ): PrimaryKey[E, K] =
    make(
      keyOf,
      List(name1, name2, name3, name4),
      key =>
        KeyFields.of(
          name1 -> e1.encode(value1(key)),
          name2 -> e2.encode(value2(key)),
          name3 -> e3.encode(value3(key)),
          name4 -> e4.encode(value4(key)),
        ),
    )

  extension [E, K](entity: E)(using pk: PrimaryKey[E, K]) def primaryKeyFilter: Filter[E] = pk.eqFilter(pk.key(entity))

  extension [E, K](entities: List[E])(using pk: PrimaryKey[E, K]) def primaryKeysFilter: Filter[E] = pk.inFilter(entities.map(pk.key))
