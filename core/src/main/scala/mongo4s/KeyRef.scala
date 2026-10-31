package mongo4s

import org.bson.{BsonObjectId, BsonValue}
import org.bson.types.ObjectId

import mongo4s.bson.{BsonEncoder, BsonField}
import mongo4s.operations.{Filter, Sort}

trait KeyRef[E, K]:
  def fieldNames: List[String]

  def fields(key: K): KeyFields

  def keyPaths: List[FieldPath] = fieldNames.map(FieldPath.literal)

  private def located(key: K): List[(FieldPath, BsonValue)] = keyPaths.zip(fields(key).toList.map(_._2))

  def eqFilter(key: K): Filter[E] =
    Filter.and(
      located(key).map { (path, value) =>
        Filter.Eq[E](path, value)
      }*
    )

  def keyOrder: Sort[E] = keyOrder(descending = false)

  def keyOrder(descending: Boolean): Sort[E] =
    keyPaths.foldLeft(Sort.empty[E]) { (sort, path) =>
      if descending
      then sort.desc(Field[E, Any](path))
      else sort.asc(Field[E, Any](path))
    }

  def afterFilter(key: K): Filter[E] = afterFilter(key, descending = false)

  def afterFilter(key: K, descending: Boolean): Filter[E] =
    val entries = located(key)

    Filter.or(
      entries.indices.toList.map { position =>
        val settled       = entries.take(position).map((path, value) => Filter.Eq[E](path, value))
        val (path, value) = entries(position)
        val beyond        = if descending then Filter.Lt[E](path, value) else Filter.Gt[E](path, value)

        Filter.and((settled :+ beyond)*)
      }*
    )
  end afterFilter

  def inFilter(keys: List[K]): Filter[E] =
    keys match
      case Nil           => Filter.none
      case single :: Nil => eqFilter(single)
      case many          =>
        val encoded = many.map(located)

        if encoded.head.size == 1
        then
          Filter.In(
            encoded.head.head._1,
            encoded.map(_.head._2),
          )
        else
          Filter.or(
            encoded.map { entry =>
              Filter.and(
                entry.map { (path, value) =>
                  Filter.Eq[E](path, value)
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
      def fieldNames: List[String]         = List(BsonField.Id)
      def fields(key: ObjectId): KeyFields = KeyFields.one(BsonField.Id, BsonObjectId(key))
