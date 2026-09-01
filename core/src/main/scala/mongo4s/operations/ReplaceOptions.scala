package mongo4s.operations

final class ReplaceOptions private (
    val upsert: Boolean
):
  def withUpsert: ReplaceOptions = new ReplaceOptions(upsert = true)

object ReplaceOptions:
  val default: ReplaceOptions = new ReplaceOptions(upsert = false)
  val upsert: ReplaceOptions  = default.withUpsert
