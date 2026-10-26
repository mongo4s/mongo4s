package mongo4s.operations

final class InsertOneOptions private (
    val comment: Option[String],
    val bypassDocumentValidation: Option[Boolean],
):
  def withComment(value: String): InsertOneOptions = copy(comment = Some(value))

  def withBypassDocumentValidation(value: Boolean): InsertOneOptions = copy(bypassDocumentValidation = Some(value))

  private def copy(
      comment: Option[String] = comment,
      bypassDocumentValidation: Option[Boolean] = bypassDocumentValidation,
  ): InsertOneOptions =
    new InsertOneOptions(
      comment = comment,
      bypassDocumentValidation = bypassDocumentValidation,
    )

object InsertOneOptions:
  val default: InsertOneOptions =
    new InsertOneOptions(
      comment = None,
      bypassDocumentValidation = None,
    )
