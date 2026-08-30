package mongo4s

import org.bson.BsonObjectId
import org.bson.types.ObjectId

import mongo4s.bson.BsonEncoder
import mongo4s.operations.Filter

trait KeyRef[E, K]:
  def fieldNames: List[String]

  def fields(key: K): KeyFields

  def eqFilter(key: K): Filter[E] =
    Filter.and(
      fields(key).toList.map { (name, value) =>
        Filter.Eq[E](FieldPath.literal(name), value)
      }*
    )
    
  def inFilter(keys: List[K]): Filter[E] =
    keys match
      case Nil           => Filter.none
      case single :: Nil => eqFilter(single)
      case many          =>
        val encoded = many.map(fields)

        if encoded.head.size == 1
        then
          Filter.In(
            FieldPath.literal(encoded.head.head._1),
            encoded.map(_.head._2),
          )
        else
          Filter.or(
            encoded.map { entry =>
              Filter.and(
                entry.toList.map { (n, v) =>
                  Filter.Eq[E](FieldPath.literal(n), v)
                }*
              )
            }*
          )
  end inFilter

object KeyRef:
  inline def apply[E, K](using instance: KeyRef[E, K]): instance.type = instance

  def field[E, K](name: String)(using encoder: BsonEncoder[K]): KeyRef[E, K] =
    new KeyRef[E, K]:
      def fieldNames: List[String]  = List(name)
      def fields(key: K): KeyFields = KeyFields.one(name, encoder.encode(key))

  def objectId[E]: KeyRef[E, ObjectId] =
    new KeyRef[E, ObjectId]:
      def fieldNames: List[String]         = List("_id")
      def fields(key: ObjectId): KeyFields = KeyFields.one("_id", BsonObjectId(key))
