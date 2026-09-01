package mongo4s.operations

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class ReplaceOptions private (
    val upsert: Boolean,
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def withUpsert: ReplaceOptions = copy(upsert = true)

  def withCollation(value: Collation): ReplaceOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): ReplaceOptions = copy(hint = Some(value))

  def withComment(value: String): ReplaceOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): ReplaceOptions = copy(bypassDocumentValidation = Some(value))

  private def copy(
      upsert: Boolean = upsert,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): ReplaceOptions =
    new ReplaceOptions(upsert, collation, hint, comment, bypassDocumentValidation)

object ReplaceOptions:
  val default: ReplaceOptions =
    new ReplaceOptions(
      upsert = false,
      collation = None,
      hint = None,
      comment = None,
      bypassDocumentValidation = None,
    )

  val upsert: ReplaceOptions = default.withUpsert
