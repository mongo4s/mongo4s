package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

enum Projection[E]:
  case Everything[T]()                                      extends Projection[T]
  case Include[T](fields: List[FieldPath], withId: Boolean) extends Projection[T]
  case Exclude[T](fields: List[FieldPath])                  extends Projection[T]

  def include[A](field: Field[E, A]): Projection[E] =
    this match
      case Include(fields, withId) => Include(fields :+ field.path, withId)
      case Everything()            => Include(List(field.path), withId = true)
      case Exclude(_)              => Include(List(field.path), withId = true)

  def exclude[A](field: Field[E, A]): Projection[E] =
    this match
      case Exclude(fields) => Exclude(fields :+ field.path)
      case Everything()    => Exclude(List(field.path))
      case Include(_, _)   => Exclude(List(field.path))

  def withoutId: Projection[E] =
    this match
      case Include(fields, _) => Include(fields, withId = false)
      case Everything()       => Exclude(List(FieldPath.literal("_id")))
      case Exclude(fields)    =>
        val id = FieldPath.literal("_id")
        if fields.contains(id)
        then this
        else Exclude(fields :+ id)
  end withoutId

  def isEmpty: Boolean = this match
    case Everything()          => true
    case Include(fields, true) => fields.isEmpty
    case Include(_, false)     => false
    case Exclude(fields)       => fields.isEmpty

  def toBson(naming: FieldNaming): BsonDocument =
    this match
      case Everything() => BsonDocument()

      case Include(fields, withId) =>
        val document = fields.foldLeft(BsonDocument())((acc, path) => acc.append(path.render(naming), BsonInt32(1)))
        if withId
        then document
        else document.append("_id", BsonInt32(0))

      case Exclude(fields) =>
        fields.foldLeft(BsonDocument())((acc, path) => acc.append(path.render(naming), BsonInt32(0)))

object Projection:
  def empty[E]: Projection[E]     = Everything()
  def excludeId[E]: Projection[E] = Exclude(List(FieldPath.literal("_id")))
