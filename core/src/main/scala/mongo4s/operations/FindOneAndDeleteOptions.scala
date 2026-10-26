package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class FindOneAndDeleteOptions[E] private (
    val sort: Sort[E],
    val projection: Projection[E],
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val maxTime: Option[FiniteDuration],
    val comment: Option[String],
    val let: Option[BsonDocument],
):
  def withSort(value: Sort[E]): FindOneAndDeleteOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndDeleteOptions[E] = copy(projection = value)

  def withCollation(value: Collation): FindOneAndDeleteOptions[E] = copy(collation = Some(value))

  def withHint(value: BsonDocument): FindOneAndDeleteOptions[E] = copy(hint = Some(value))

  def withMaxTime(value: FiniteDuration): FindOneAndDeleteOptions[E] = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))

  def withComment(value: String): FindOneAndDeleteOptions[E] = copy(comment = Some(value))

  def withLet(variables: BsonDocument): FindOneAndDeleteOptions[E] = copy(let = Some(variables))

  private def copy(
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      maxTime: Option[FiniteDuration] = maxTime,
      comment: Option[String] = comment,
      let: Option[BsonDocument] = let,
  ): FindOneAndDeleteOptions[E] =
    new FindOneAndDeleteOptions(sort, projection, collation, hint, maxTime, comment, let)

object FindOneAndDeleteOptions:
  def default[E]: FindOneAndDeleteOptions[E] =
    new FindOneAndDeleteOptions(
      sort = Sort.empty[E],
      projection = Projection.empty[E],
      collation = None,
      hint = None,
      maxTime = None,
      comment = None,
      let = None,
    )
