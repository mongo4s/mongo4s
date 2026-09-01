package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.{BsonDocument, BsonValue, BsonInt32, BsonString}
import com.mongodb.client.model.Collation

import mongo4s.bson.FieldNaming
import mongo4s.{Field, FieldPath}

final class Index[E] private (
    val keys: List[(FieldPath, Index.Direction)],
    val unique: Boolean,
    val sparse: Boolean,
    val hidden: Boolean,
    val name: Option[String],
    val expireAfter: Option[FiniteDuration],
    val partialFilter: Option[Filter[E]],
    val collation: Option[Collation],
    val wildcardProjection: Option[BsonDocument],
):
  def ascending[A](field: Field[E, A]): Index[E]   = copy(keys = keys :+ (field.path, Index.Direction.Ascending))
  def descending[A](field: Field[E, A]): Index[E]  = copy(keys = keys :+ (field.path, Index.Direction.Descending))
  def text[A](field: Field[E, A]): Index[E]        = copy(keys = keys :+ (field.path, Index.Direction.Text))
  def hashed[A](field: Field[E, A]): Index[E]      = copy(keys = keys :+ (field.path, Index.Direction.Hashed))
  def geo2dsphere[A](field: Field[E, A]): Index[E] = copy(keys = keys :+ (field.path, Index.Direction.Geo2dsphere))
  def geo2d[A](field: Field[E, A]): Index[E]       = copy(keys = keys :+ (field.path, Index.Direction.Geo2d))

  def withUnique: Index[E] = copy(unique = true)
  def withSparse: Index[E] = copy(sparse = true)
  def withHidden: Index[E] = copy(hidden = true)

  def named(value: String): Index[E] = copy(name = Some(value))

  def where(filter: Filter[E]): Index[E] = copy(partialFilter = Some(filter))

  def withCollation(value: Collation): Index[E] = copy(collation = Some(value))

  def withWildcardProjection(value: BsonDocument): Index[E] = copy(wildcardProjection = Some(value))

  def expiringAfter(duration: FiniteDuration): Index[E] =
    require(
      duration.toSeconds > 0,
      s"TTL must be at least one second — MongoDB stores expireAfterSeconds as a whole number, and $duration would truncate to 0 (expire immediately)",
    )
    copy(expireAfter = Some(duration))

  def keysToBson(naming: FieldNaming): BsonDocument =
    keys.reverse.distinctBy(_._1).reverse.foldLeft(BsonDocument()) { (document, entry) =>
      val (path, direction) = entry

      document.append(path.render(naming), direction.toBson)
    }

  private def copy(
      keys: List[(FieldPath, Index.Direction)] = keys,
      unique: Boolean = unique,
      sparse: Boolean = sparse,
      hidden: Boolean = hidden,
      name: Option[String] = name,
      expireAfter: Option[FiniteDuration] = expireAfter,
      partialFilter: Option[Filter[E]] = partialFilter,
      collation: Option[Collation] = collation,
      wildcardProjection: Option[BsonDocument] = wildcardProjection,
  ): Index[E] =
    new Index(keys, unique, sparse, hidden, name, expireAfter, partialFilter, collation, wildcardProjection)

object Index:

  enum Direction:
    case Ascending, Descending, Text, Hashed, Geo2dsphere, Geo2d

    def toBson: BsonValue =
      this match
        case Ascending   => BsonInt32(1)
        case Descending  => BsonInt32(-1)
        case Text        => BsonString("text")
        case Hashed      => BsonString("hashed")
        case Geo2dsphere => BsonString("2dsphere")
        case Geo2d       => BsonString("2d")

  def empty[E]: Index[E] =
    new Index(
      keys = Nil,
      unique = false,
      sparse = false,
      hidden = false,
      name = None,
      expireAfter = None,
      partialFilter = None,
      collation = None,
      wildcardProjection = None,
    )

  def ascending[E, A](field: Field[E, A]): Index[E]   = empty[E].ascending(field)
  def descending[E, A](field: Field[E, A]): Index[E]  = empty[E].descending(field)
  def hashed[E, A](field: Field[E, A]): Index[E]      = empty[E].hashed(field)
  def geo2dsphere[E, A](field: Field[E, A]): Index[E] = empty[E].geo2dsphere(field)
  def unique[E, A](field: Field[E, A]): Index[E]      = empty[E].ascending(field).withUnique

  def forKeyFields[E](names: List[String]): Index[E] =
    names.foldLeft(empty[E])((index, name) => index.ascending(Field.stored[E, Any](name))).withUnique
