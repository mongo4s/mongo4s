package mongo4s.operations

import org.bson.BsonDocument

import com.mongodb.client.model.Collation

final class DeleteOptions private (
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val comment: Option[String],
    val let: Option[BsonDocument],
):
  def withCollation(value: Collation): DeleteOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): DeleteOptions = copy(hint = Some(value))

  def withComment(value: String): DeleteOptions = copy(comment = Some(value))

  def withLet(variables: BsonDocument): DeleteOptions = copy(let = Some(variables))

  private def copy(
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      comment: Option[String] = comment,
      let: Option[BsonDocument] = let,
  ): DeleteOptions =
    new DeleteOptions(
      collation = collation,
      hint = hint,
      comment = comment,
      let = let,
    )

object DeleteOptions:
  val default: DeleteOptions =
    new DeleteOptions(
      collation = None,
      hint = None,
      comment = None,
      let = None,
    )
