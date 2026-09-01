package mongo4s.operations

final class OutOptions private (
    val database: Option[String]
):
  def inDatabase(value: String): OutOptions = new OutOptions(database = Some(value))

  def isEmpty: Boolean = database.isEmpty

object OutOptions:
  val default: OutOptions = new OutOptions(database = None)
