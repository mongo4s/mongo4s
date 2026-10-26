package mongo4s.operations

import org.bson.BsonDocument

import com.mongodb.client.model.Collation

final class ReplaceOptions private (
    val upsert: Boolean,
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
    val sort: Option[Sort[?]],
    val let: Option[BsonDocument],
):
  def withUpsert: ReplaceOptions = copy(upsert = true)

  def withCollation(value: Collation): ReplaceOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): ReplaceOptions = copy(hint = Some(value))

  def withComment(value: String): ReplaceOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): ReplaceOptions = copy(bypassDocumentValidation = Some(value))

  def withSort(value: Sort[?]): ReplaceOptions = copy(sort = Some(value))

  def withLet(variables: BsonDocument): ReplaceOptions = copy(let = Some(variables))

  private def copy(
      upsert: Boolean = upsert,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
      sort: Option[Sort[?]] = sort,
      let: Option[BsonDocument] = let,
  ): ReplaceOptions =
    new ReplaceOptions(upsert, collation, hint, comment, bypassDocumentValidation, sort, let)

object ReplaceOptions:
  val default: ReplaceOptions =
    new ReplaceOptions(
      upsert = false,
      collation = None,
      hint = None,
      comment = None,
      bypassDocumentValidation = None,
      sort = None,
      let = None,
    )

  val upsert: ReplaceOptions = default.withUpsert
