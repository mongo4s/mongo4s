package mongo4s.operations

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class UpdateOptions private (
    val upsert: Boolean,
    val arrayFilters: Seq[Filter[?]],
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def withUpsert: UpdateOptions = copy(upsert = true)

  def withArrayFilters(filters: Seq[Filter[?]]): UpdateOptions = copy(arrayFilters = filters)

  def withCollation(value: Collation): UpdateOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): UpdateOptions = copy(hint = Some(value))

  def withComment(value: String): UpdateOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): UpdateOptions = copy(bypassDocumentValidation = Some(value))

  private def copy(
      upsert: Boolean = upsert,
      arrayFilters: Seq[Filter[?]] = arrayFilters,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): UpdateOptions =
    new UpdateOptions(
      upsert = upsert,
      arrayFilters = arrayFilters,
      collation = collation,
      hint = hint,
      comment = comment,
      bypassDocumentValidation = bypassDocumentValidation,
    )

object UpdateOptions:
  val default: UpdateOptions =
    new UpdateOptions(
      upsert = false,
      arrayFilters = Nil,
      collation = None,
      hint = None,
      comment = None,
      bypassDocumentValidation = None,
    )

  val upsert: UpdateOptions = default.withUpsert
