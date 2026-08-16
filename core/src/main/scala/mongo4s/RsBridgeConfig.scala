package mongo4s

import scala.concurrent.duration.FiniteDuration

final case class RsBridgeConfig(
    bufferSize: Int,
    timeout: Option[FiniteDuration] = None,
    strictSingleResult: Boolean = false,
)

object RsBridgeConfig:
  val Default: RsBridgeConfig = RsBridgeConfig(bufferSize = 256)

  given default: RsBridgeConfig = Default
