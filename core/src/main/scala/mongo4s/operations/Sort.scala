package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

final case class Sort[E](fields: List[(FieldPath, Boolean)]):
  def asc[A](field: Field[E, A]): Sort[E]  = Sort(fields :+ (field.path -> true))
  def desc[A](field: Field[E, A]): Sort[E] = Sort(fields :+ (field.path -> false))

  def isEmpty: Boolean = fields.isEmpty

  def toBson(naming: FieldNaming): BsonDocument =
    val ordered =
      if fields.lengthIs > 1
      then fields.reverse.distinctBy(_._1).reverse
      else fields

    ordered.foldLeft(BsonDocument()) { (document, entry) =>
      document.append(
        entry._1.render(naming),
        BsonInt32(if entry._2 then 1 else -1),
      )
    }

object Sort:
  def empty[E]: Sort[E]                       = Sort(Nil)
  def asc[E, A](field: Field[E, A]): Sort[E]  = empty[E].asc(field)
  def desc[E, A](field: Field[E, A]): Sort[E] = empty[E].desc(field)
