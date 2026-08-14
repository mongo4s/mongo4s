package mongo4s

import org.bson.types.ObjectId
import org.bson.{BsonObjectId, BsonValue}

import mongo4s.bson.BsonEncoder
import mongo4s.operations.Filter

trait PrimaryKey[E, K] extends KeyRef[E, K]:
  def key(entity: E): K

object PrimaryKey:
  inline def apply[E, K](using instance: PrimaryKey[E, K]): instance.type = instance

  def make[E, K](keyOf: E => K, fieldsOf: K => KeyFields): PrimaryKey[E, K] =
    new PrimaryKey[E, K]:
      def key(entity: E): K         = keyOf(entity)
      def fields(key: K): KeyFields = fieldsOf(key)

  def id[E, Id](keyOf: E => Id)(using encoder: BsonEncoder[Id]): PrimaryKey[E, Id] =
    make(keyOf, key => KeyFields.one("id", encoder.encode(key)))

  def objectId[E](keyOf: E => ObjectId): PrimaryKey[E, ObjectId] =
    make(keyOf, key => KeyFields.one("_id", BsonObjectId(key)))

  def storedId[E, Id](keyOf: E => Id)(using encoder: BsonEncoder[Id]): PrimaryKey[E, Id] =
    make(keyOf, key => KeyFields.one("_id", encoder.encode(key)))

  def single[E, F1](name: String)(keyOf: E => F1)(using encoder: BsonEncoder[F1]): PrimaryKey[E, F1] =
    make(keyOf, key => KeyFields.one(name, encoder.encode(key)))

  def make[E, K, F1, F2](
      keyOf: E => K,
      field1: K => (String, F1),
      field2: K => (String, F2),
  )(using e1: BsonEncoder[F1], e2: BsonEncoder[F2]): PrimaryKey[E, K] =
    make(keyOf, key => KeyFields.of(encoded(field1, key), encoded(field2, key)))

  def make[E, K, F1, F2, F3](
      keyOf: E => K,
      field1: K => (String, F1),
      field2: K => (String, F2),
      field3: K => (String, F3),
  )(using e1: BsonEncoder[F1], e2: BsonEncoder[F2], e3: BsonEncoder[F3]): PrimaryKey[E, K] =
    make(keyOf, key => KeyFields.of(encoded(field1, key), encoded(field2, key), encoded(field3, key)))

  def make[E, K, F1, F2, F3, F4](
      keyOf: E => K,
      field1: K => (String, F1),
      field2: K => (String, F2),
      field3: K => (String, F3),
      field4: K => (String, F4),
  )(using e1: BsonEncoder[F1], e2: BsonEncoder[F2], e3: BsonEncoder[F3], e4: BsonEncoder[F4]): PrimaryKey[E, K] =
    make(keyOf, key => KeyFields.of(encoded(field1, key), encoded(field2, key), encoded(field3, key), encoded(field4, key)))

  private def encoded[K, F](extract: K => (String, F), key: K)(using encoder: BsonEncoder[F]): (String, BsonValue) =
    val (name, value) = extract(key)
    name -> encoder.encode(value)

  extension [E, K](entity: E)(using pk: PrimaryKey[E, K]) def primaryKeyFilter: Filter[E] = pk.eqFilter(pk.key(entity))

  extension [E, K](entities: List[E])(using pk: PrimaryKey[E, K]) def primaryKeysFilter: Filter[E] = pk.inFilter(entities.map(pk.key))
