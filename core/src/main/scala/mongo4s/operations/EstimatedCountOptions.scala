package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

final class EstimatedCountOptions private (
    val maxTime: Option[FiniteDuration],
    val comment: Option[String],
):
  def withMaxTime(value: FiniteDuration): EstimatedCountOptions = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))

  def withComment(value: String): EstimatedCountOptions = copy(comment = Some(value))

  private def copy(
      maxTime: Option[FiniteDuration] = maxTime,
      comment: Option[String] = comment,
  ): EstimatedCountOptions =
    new EstimatedCountOptions(
      maxTime = maxTime,
      comment = comment,
    )

object EstimatedCountOptions:
  val default: EstimatedCountOptions =
    new EstimatedCountOptions(
      maxTime = None,
      comment = None,
    )
