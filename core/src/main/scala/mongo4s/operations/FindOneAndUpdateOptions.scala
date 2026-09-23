package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class FindOneAndUpdateOptions[E] private (
    val returnUpdated: Boolean,
    val upsert: Boolean,
    val sort: Sort[E],
    val projection: Projection[E],
    val arrayFilters: Seq[Filter[?]],
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val maxTime: Option[FiniteDuration],
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def returningPrevious: FindOneAndUpdateOptions[E] = copy(returnUpdated = false)

  def withUpsert: FindOneAndUpdateOptions[E] = copy(upsert = true)

  def withSort(value: Sort[E]): FindOneAndUpdateOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndUpdateOptions[E] = copy(projection = value)

  def withArrayFilters(filters: Seq[Filter[?]]): FindOneAndUpdateOptions[E] = copy(arrayFilters = filters)

  def withCollation(value: Collation): FindOneAndUpdateOptions[E] = copy(collation = Some(value))

  def withHint(value: BsonDocument): FindOneAndUpdateOptions[E] = copy(hint = Some(value))

  def withMaxTime(value: FiniteDuration): FindOneAndUpdateOptions[E] = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))

  def withComment(value: String): FindOneAndUpdateOptions[E] = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): FindOneAndUpdateOptions[E] = copy(bypassDocumentValidation = Some(value))

  private def copy(
      returnUpdated: Boolean = returnUpdated,
      upsert: Boolean = upsert,
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
      arrayFilters: Seq[Filter[?]] = arrayFilters,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      maxTime: Option[FiniteDuration] = maxTime,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): FindOneAndUpdateOptions[E] =
    new FindOneAndUpdateOptions(returnUpdated, upsert, sort, projection, arrayFilters, collation, hint, maxTime, comment, bypassDocumentValidation)

object FindOneAndUpdateOptions:
  def default[E]: FindOneAndUpdateOptions[E] =
    new FindOneAndUpdateOptions(
      returnUpdated = true,
      upsert = false,
      sort = Sort.empty[E],
      projection = Projection.empty[E],
      arrayFilters = Nil,
      collation = None,
      hint = None,
      maxTime = None,
      comment = None,
      bypassDocumentValidation = None,
    )

  def upsert[E]: FindOneAndUpdateOptions[E] = default[E].withUpsert
