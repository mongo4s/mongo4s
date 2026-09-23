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
):
  def withCollation(value: Collation): CountOptions = copy(collation = Some(value))

  def withHint(value: BsonDocument): CountOptions = copy(hint = Some(value))

  def withLimit(value: Int): CountOptions = copy(limit = Some(value))

  def withSkip(value: Int): CountOptions = copy(skip = Some(value))

  def withMaxTime(value: FiniteDuration): CountOptions =
    require(
      value.toMillis > 0,
      s"maxTime must be at least one millisecond — MongoDB takes it in whole milliseconds, and $value would truncate to 0, which it reads as no limit",
    )
    copy(maxTime = Some(value))
  end withMaxTime

  private def copy(
      collation: Option[Collation] = collation,
      hint: Option[BsonDocument] = hint,
      limit: Option[Int] = limit,
      skip: Option[Int] = skip,
      maxTime: Option[FiniteDuration] = maxTime,
  ): CountOptions =
    new CountOptions(
      collation = collation,
      hint = hint,
      limit = limit,
      skip = skip,
      maxTime = maxTime,
    )

object CountOptions:
  val default: CountOptions =
    new CountOptions(
      collation = None,
      hint = None,
      limit = None,
      skip = None,
      maxTime = None,
    )
