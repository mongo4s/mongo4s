package mongo4s

import scala.util.control.NoStackTrace
import scala.concurrent.duration.FiniteDuration

sealed abstract class RsBridgeError(message: String) extends RuntimeException(message), NoStackTrace

object RsBridgeError:
  final case class EmptyResult()                  extends RsBridgeError("Expected exactly one result but got none")
  final case class TooManyResults()               extends RsBridgeError("Expected exactly one result but got more than one")
  final case class Timeout(after: FiniteDuration) extends RsBridgeError(s"Operation timed out after $after")
