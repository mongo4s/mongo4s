package mongo4s

final case class RsBridgeConfig(bufferSize: Int)

object RsBridgeConfig:
  val Default: RsBridgeConfig = RsBridgeConfig(bufferSize = 256)

  given default: RsBridgeConfig = Default
