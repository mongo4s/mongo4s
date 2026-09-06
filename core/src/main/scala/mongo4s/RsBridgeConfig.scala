package mongo4s

import scala.concurrent.duration.FiniteDuration

final class RsBridgeConfig private (
    val bufferSize: Int,
    val timeout: Option[FiniteDuration],
    val strictSingleResult: Boolean,
):
  require(bufferSize > 0, s"RsBridgeConfig.bufferSize must be positive, got $bufferSize")

  def withBufferSize(value: Int): RsBridgeConfig = copy(bufferSize = value)

  def withTimeout(value: FiniteDuration): RsBridgeConfig = copy(timeout = Some(value))

  def withoutTimeout: RsBridgeConfig = copy(timeout = None)

  def withStrictSingleResult: RsBridgeConfig = copy(strictSingleResult = true)

  private def copy(
      bufferSize: Int = bufferSize,
      timeout: Option[FiniteDuration] = timeout,
      strictSingleResult: Boolean = strictSingleResult,
  ): RsBridgeConfig =
    new RsBridgeConfig(
      bufferSize = bufferSize,
      timeout = timeout,
      strictSingleResult = strictSingleResult,
    )

object RsBridgeConfig:
  val default: RsBridgeConfig =
    new RsBridgeConfig(
      bufferSize = 256,
      timeout = None,
      strictSingleResult = false,
    )

  given RsBridgeConfig = default
