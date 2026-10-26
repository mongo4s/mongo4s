package mongo4s.operations

import org.bson.BsonDocument

final class BulkWriteOptions private (
    val ordered: Boolean,
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
    val let: Option[BsonDocument],
):
  def withUnordered: BulkWriteOptions = copy(ordered = false)

  def withComment(value: String): BulkWriteOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): BulkWriteOptions = copy(bypassDocumentValidation = Some(value))

  def withLet(variables: BsonDocument): BulkWriteOptions = copy(let = Some(variables))

  private def copy(
      ordered: Boolean = ordered,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
      let: Option[BsonDocument] = let,
  ): BulkWriteOptions =
    new BulkWriteOptions(
      ordered = ordered,
      comment = comment,
      bypassDocumentValidation = bypassDocumentValidation,
      let = let,
    )

object BulkWriteOptions:
  val default: BulkWriteOptions =
    new BulkWriteOptions(
      ordered = true,
      comment = None,
      bypassDocumentValidation = None,
      let = None,
    )

  val unordered: BulkWriteOptions = default.withUnordered
