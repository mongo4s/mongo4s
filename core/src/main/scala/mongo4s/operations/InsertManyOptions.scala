package mongo4s.operations

final class InsertManyOptions private (
    val ordered: Boolean,
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def withUnordered: InsertManyOptions = copy(ordered = false)

  def withComment(value: String): InsertManyOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): InsertManyOptions = copy(bypassDocumentValidation = Some(value))

  private def copy(
      ordered: Boolean = ordered,
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): InsertManyOptions =
    new InsertManyOptions(
      ordered = ordered,
      comment = comment,
      bypassDocumentValidation = bypassDocumentValidation,
    )

object InsertManyOptions:
  val default: InsertManyOptions =
    new InsertManyOptions(
      ordered = true,
      comment = None,
      bypassDocumentValidation = None,
    )

  val unordered: InsertManyOptions = default.withUnordered
