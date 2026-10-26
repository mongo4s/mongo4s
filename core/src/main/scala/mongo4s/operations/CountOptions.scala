package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

final class CountOptions private (
    val collation: Option[Collation],
    val hint: Option[BsonDocument],
    val limit: Option[Int],
    val skip: Option[Int],
    val maxTime: Option[FiniteDuration],
    val comment: Option[String],
):
  def withCollation(value: Collation): CountOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): CountOptions = copy(hint = Some(value))

  def withLimit(value: Int): CountOptions = copy(limit = Some(Bounds.atLeastOne(value, "limit", Bounds.NoLimit)))

  def withSkip(value: Int): CountOptions = copy(skip = Some(Bounds.notNegative(value, "skip")))

  def withMaxTime(value: FiniteDuration): CountOptions = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))

  def withComment(value: String): CountOptions = copy(comment = Some(value))

  private def copy(
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      limit: Option[Int] = limit,
      skip: Option[Int] = skip,
      maxTime: Option[FiniteDuration] = maxTime,
      comment: Option[String] = comment,
  ): CountOptions =
    new CountOptions(
      collation = collation,
      hint = hint,
      limit = limit,
      skip = skip,
      maxTime = maxTime,
      comment = comment,
    )

object CountOptions:
  val default: CountOptions =
    new CountOptions(
      collation = None,
      hint = None,
      limit = None,
      skip = None,
      maxTime = None,
      comment = None,
    )
