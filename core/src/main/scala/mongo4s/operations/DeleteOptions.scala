package mongo4s.operations

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class DeleteOptions private (
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val comment: Option[String],
):
  def withCollation(value: Collation): DeleteOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): DeleteOptions = copy(hint = Some(value))

  def withComment(value: String): DeleteOptions = copy(comment = Some(value))

  private def copy(
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      comment: Option[String] = comment,
  ): DeleteOptions =
    new DeleteOptions(
      collation = collation,
      hint = hint,
      comment = comment
    )

object DeleteOptions:
  val default: DeleteOptions =
    new DeleteOptions(
      collation = None,
      hint = None,
      comment = None,
    )
