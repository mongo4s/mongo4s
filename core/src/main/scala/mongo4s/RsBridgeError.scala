package mongo4s

import scala.concurrent.duration.FiniteDuration

sealed abstract class RsBridgeError(message: String) extends RuntimeException(message)

object RsBridgeError:
  final case class EmptyResult()                  extends RsBridgeError("Expected exactly one result but got none")
  final case class TooManyResults(count: Int)     extends RsBridgeError(s"Expected exactly one result but got $count")
  final case class Timeout(after: FiniteDuration) extends RsBridgeError(s"Operation timed out after $after")
