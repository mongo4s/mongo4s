package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class FindOneAndReplaceOptions[E] private (
    val returnUpdated: Boolean,
    val upsert: Boolean,
    val sort: Sort[E],
    val projection: Projection[E],
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val maxTime: Option[FiniteDuration],
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def returningPrevious: FindOneAndReplaceOptions[E] = copy(returnUpdated = false)

  def withUpsert: FindOneAndReplaceOptions[E] = copy(upsert = true)

  def withSort(value: Sort[E]): FindOneAndReplaceOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndReplaceOptions[E] = copy(projection = value)

  def withCollation(value: Collation): FindOneAndReplaceOptions[E] = copy(collation = Some(value))

  def withHint(value: BsonDocument): FindOneAndReplaceOptions[E] = copy(hint = Some(value))

  def withMaxTime(value: FiniteDuration): FindOneAndReplaceOptions[E] = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))

  def withComment(value: String): FindOneAndReplaceOptions[E] = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): FindOneAndReplaceOptions[E] = copy(bypassDocumentValidation = Some(value))

  private def copy(
      returnUpdated: Boolean = returnUpdated,
      upsert: Boolean = upsert,
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      maxTime: Option[FiniteDuration] = maxTime,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): FindOneAndReplaceOptions[E] =
    new FindOneAndReplaceOptions(returnUpdated, upsert, sort, projection, collation, hint, maxTime, comment, bypassDocumentValidation)

object FindOneAndReplaceOptions:
  def default[E]: FindOneAndReplaceOptions[E] =
    new FindOneAndReplaceOptions(
      returnUpdated = true,
      upsert = false,
      sort = Sort.empty[E],
      projection = Projection.empty[E],
      collation = None,
      hint = None,
      maxTime = None,
      comment = None,
      bypassDocumentValidation = None,
    )

  def upsert[E]: FindOneAndReplaceOptions[E] = default[E].withUpsert
