package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32}

import mongo4s.bson.FieldNaming
import mongo4s.{ElementOf, Field, FieldPath}

enum Projection[E]:

  case Everything[T](slices: List[(FieldPath, Slice)] = Nil) extends Projection[T]

  case Include[T](
      fields: List[FieldPath],
      withId: Boolean,
      slices: List[(FieldPath, Slice)] = Nil,
  ) extends Projection[T]

  case Exclude[T](
      fields: List[FieldPath],
      slices: List[(FieldPath, Slice)] = Nil
  ) extends Projection[T]

  def slices: List[(FieldPath, Slice)]

  def isEmpty: Boolean = this match
    case Everything(slices)            => slices.isEmpty
    case Include(fields, true, slices) => fields.isEmpty && slices.isEmpty
    case Include(_, false, _)          => false
    case Exclude(fields, slices)       => fields.isEmpty && slices.isEmpty

  def toBson(naming: FieldNaming): BsonDocument =
    val document = this match
      case Everything(_) => BsonDocument()

      case Include(fields, withId, _) =>
        val included = fields.foldLeft(BsonDocument()) { (acc, path) =>
          acc.append(path.render(naming), BsonInt32(1))
        }
        if withId
        then included
        else included.append("_id", BsonInt32(0))

      case Exclude(fields, _) =>
        fields.foldLeft(BsonDocument()) { (acc, path) =>
          acc.append(path.render(naming), BsonInt32(0))
        }

    slices.foldLeft(document) { (acc, entry) =>
      acc.append(entry._1.render(naming), BsonDocument("$slice", entry._2.toBson))
    }
  end toBson

object Projection:

  private val IdPath: FieldPath = FieldPath.literal("_id")

  def empty[E]: Everything[E]  = Everything()
  def excludeId[E]: Exclude[E] = Exclude(List(IdPath))

  extension [E](projection: Everything[E])
    def include[A](field: Field[E, A]): Include[E] = Include(List(field.path), withId = true, projection.slices)
    def exclude[A](field: Field[E, A]): Exclude[E] = Exclude(List(field.path), projection.slices)
    def withoutId: Exclude[E]                      = Exclude(List(IdPath), projection.slices)

    def slice[C](field: Field[E, C], count: Int)(using ElementOf[C, ?]): Everything[E] =
      Everything(projection.slices :+ (field.path -> Slice(count, None)))

    def sliceFrom[C](field: Field[E, C], skip: Int, count: Int)(using ElementOf[C, ?]): Everything[E] =
      Everything(projection.slices :+ (field.path -> Slice(count, Some(skip))))

  extension [E](projection: Include[E])
    def include[A](field: Field[E, A]): Include[E] =
      Include(projection.fields :+ field.path, projection.withId, projection.slices)

    def withoutId: Include[E] = Include(projection.fields, withId = false, projection.slices)

    def slice[C](field: Field[E, C], count: Int)(using ElementOf[C, ?]): Include[E] =
      Include(projection.fields, projection.withId, projection.slices :+ (field.path -> Slice(count, None)))

    def sliceFrom[C](field: Field[E, C], skip: Int, count: Int)(using ElementOf[C, ?]): Include[E] =
      Include(projection.fields, projection.withId, projection.slices :+ (field.path -> Slice(count, Some(skip))))

  extension [E](projection: Exclude[E])
    def exclude[A](field: Field[E, A]): Exclude[E] = Exclude(projection.fields :+ field.path, projection.slices)

    def withoutId: Exclude[E] =
      if projection.fields.contains(IdPath)
      then projection
      else Exclude(projection.fields :+ IdPath, projection.slices)

    def slice[C](field: Field[E, C], count: Int)(using ElementOf[C, ?]): Exclude[E] =
      Exclude(projection.fields, projection.slices :+ (field.path -> Slice(count, None)))

    def sliceFrom[C](field: Field[E, C], skip: Int, count: Int)(using ElementOf[C, ?]): Exclude[E] =
      Exclude(projection.fields, projection.slices :+ (field.path -> Slice(count, Some(skip))))
