package mongo4s

enum ExitCase:
  case Succeeded
  case Errored(error: Throwable)
  case Canceled

object ExitCase:

  def errorOf(exitCase: ExitCase): Option[Throwable] = exitCase match
    case Errored(error) => Some(error)
    case Succeeded      => None
    case Canceled       => None
