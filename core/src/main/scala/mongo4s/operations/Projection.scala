package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

enum Projection[E]:
  case Everything[T]()                                      extends Projection[T]
  case Include[T](fields: List[FieldPath], withId: Boolean) extends Projection[T]
  case Exclude[T](fields: List[FieldPath])                  extends Projection[T]

  def isEmpty: Boolean = this match
    case Everything()          => true
    case Include(fields, true) => fields.isEmpty
    case Include(_, false)     => false
    case Exclude(fields)       => fields.isEmpty

  def toBson(naming: FieldNaming): BsonDocument =
    this match
      case Everything() => BsonDocument()

      case Include(fields, withId) =>
        val document = fields.foldLeft(BsonDocument()) { (acc, path) =>
          acc.append(path.render(naming), BsonInt32(1))
        }
        if withId
        then document
        else document.append("_id", BsonInt32(0))

      case Exclude(fields) =>
        fields.foldLeft(BsonDocument()) { (acc, path) =>
          acc.append(path.render(naming), BsonInt32(0))
        }

object Projection:
  private val IdPath: FieldPath = FieldPath.literal("_id")

  def empty[E]: Everything[E]  = Everything()
  def excludeId[E]: Exclude[E] = Exclude(List(IdPath))

  extension [E](projection: Everything[E])
    def include[A](field: Field[E, A]): Include[E] = Include(List(field.path), withId = true)
    def exclude[A](field: Field[E, A]): Exclude[E] = Exclude(List(field.path))
    def withoutId: Exclude[E]                      = Exclude(List(IdPath))

  extension [E](projection: Include[E])
    def include[A](field: Field[E, A]): Include[E] = Include(projection.fields :+ field.path, projection.withId)
    def withoutId: Include[E]                      = Include(projection.fields, withId = false)

  extension [E](projection: Exclude[E])
    def exclude[A](field: Field[E, A]): Exclude[E] = Exclude(projection.fields :+ field.path)

    def withoutId: Exclude[E] =
      if projection.fields.contains(IdPath)
      then projection
      else Exclude(projection.fields :+ IdPath)
