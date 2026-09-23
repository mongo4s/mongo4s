package mongo4s.operations

import scala.concurrent.duration.FiniteDuration

private[mongo4s] object TimeLimit:

  def millis(value: FiniteDuration, name: String, zeroMeans: String = "which it reads as no limit"): FiniteDuration =
    require(
      value.toMillis > 0,
      s"$name must be at least one millisecond — MongoDB takes it in whole milliseconds, and $value would truncate to 0, $zeroMeans",
    )
    value
