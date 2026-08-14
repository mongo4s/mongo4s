package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

final case class Projection[E](included: List[FieldPath] = Nil, excluded: List[FieldPath] = Nil, excludeId: Boolean = false):
  def include[A](field: Field[E, A]): Projection[E] = copy(included = included :+ field.path)
  def exclude[A](field: Field[E, A]): Projection[E] = copy(excluded = excluded :+ field.path)
  def withoutId: Projection[E]                      = copy(excludeId = true)

  def isEmpty: Boolean = included.isEmpty && excluded.isEmpty && !excludeId

  def toBson(naming: FieldNaming): BsonDocument =
    val withIncluded = included.foldLeft(BsonDocument())((acc, path) => acc.append(path.render(naming), BsonInt32(1)))
    val withExcluded = excluded.foldLeft(withIncluded)((acc, path) => acc.append(path.render(naming), BsonInt32(0)))
    if excludeId then withExcluded.append("_id", BsonInt32(0)) else withExcluded

object Projection:
  def empty[E]: Projection[E]     = Projection()
  def excludeId[E]: Projection[E] = Projection(excludeId = true)
