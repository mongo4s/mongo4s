package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

enum SortOrder:
  case Ascending, Descending, TextScore

  def toBson: BsonValue = this match
    case Ascending  => BsonInt32(1)
    case Descending => BsonInt32(-1)
    case TextScore  => BsonDocument("$meta", BsonString("textScore"))

final case class Sort[E](fields: List[(FieldPath, SortOrder)]):
  def asc[A](field: Field[E, A]): Sort[E]  = Sort(fields :+ (field.path -> SortOrder.Ascending))
  def desc[A](field: Field[E, A]): Sort[E] = Sort(fields :+ (field.path -> SortOrder.Descending))

  def byTextScore(as: String = Sort.TextScoreField): Sort[E] =
    Sort(fields :+ (FieldPath.literal(as) -> SortOrder.TextScore))

  def isEmpty: Boolean = fields.isEmpty

  def toBson(naming: FieldNaming): BsonDocument =
    val ordered =
      if fields.lengthIs > 1
      then fields.reverse.distinctBy(_._1).reverse
      else fields

    ordered.foldLeft(BsonDocument()) { (document, entry) =>
      document.append(entry._1.render(naming), entry._2.toBson)
    }

object Sort:
  val TextScoreField: String = "score"

  def empty[E]: Sort[E]                       = Sort(Nil)
  def asc[E, A](field: Field[E, A]): Sort[E]  = empty[E].asc(field)
  def desc[E, A](field: Field[E, A]): Sort[E] = empty[E].desc(field)

  def byTextScore[E](as: String = TextScoreField): Sort[E] = empty[E].byTextScore(as)
