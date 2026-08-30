package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.{BsonDocument, BsonValue, BsonInt32, BsonString}

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

final case class Index[E](
    keys: List[(FieldPath, Index.Direction)],
    unique: Boolean = false,
    sparse: Boolean = false,
    name: Option[String] = None,
    expireAfter: Option[FiniteDuration] = None,
    partialFilter: Option[Filter[E]] = None,
):
  def ascending[A](field: Field[E, A]): Index[E]  = copy(keys = keys :+ (field.path, Index.Direction.Ascending))
  def descending[A](field: Field[E, A]): Index[E] = copy(keys = keys :+ (field.path, Index.Direction.Descending))
  def text[A](field: Field[E, A]): Index[E]       = copy(keys = keys :+ (field.path, Index.Direction.Text))

  def withUnique: Index[E]           = copy(unique = true)
  def withSparse: Index[E]           = copy(sparse = true)
  def named(value: String): Index[E] = copy(name = Some(value))
  
  def expiringAfter(duration: FiniteDuration): Index[E] =
    require(
      duration.toSeconds > 0,
      s"TTL must be at least one second — MongoDB stores expireAfterSeconds as a whole number, and $duration would truncate to 0 (expire immediately)",
    )
    copy(expireAfter = Some(duration))

  def where(filter: Filter[E]): Index[E] = copy(partialFilter = Some(filter))

  def keysToBson(naming: FieldNaming): BsonDocument =
    keys.foldLeft(BsonDocument()) { (document, entry) =>
      val (path, direction) = entry

      document.append(path.render(naming), direction.toBson)
    }

object Index:

  enum Direction:
    case Ascending, Descending, Text

    def toBson: BsonValue =
      this match
        case Ascending  => BsonInt32(1)
        case Descending => BsonInt32(-1)
        case Text       => BsonString("text")

  def empty[E]: Index[E] = Index(Nil)

  def ascending[E, A](field: Field[E, A]): Index[E]  = empty[E].ascending(field)
  def descending[E, A](field: Field[E, A]): Index[E] = empty[E].descending(field)
  def unique[E, A](field: Field[E, A]): Index[E]     = empty[E].ascending(field).withUnique

  def forKeyFields[E](names: List[String]): Index[E] =
    Index(
      names.map(name => (FieldPath.literal(name), Direction.Ascending)),
      unique = true,
    )
