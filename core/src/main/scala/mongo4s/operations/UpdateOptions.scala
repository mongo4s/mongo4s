package mongo4s.operations

final class UpdateOptions private (
    val upsert: Boolean,
    val arrayFilters: Seq[Filter[?]],
):
  def withUpsert: UpdateOptions = copy(upsert = true)

  def withArrayFilters(filters: Seq[Filter[?]]): UpdateOptions = copy(arrayFilters = filters)

  private def copy(
      upsert: Boolean = upsert,
      arrayFilters: Seq[Filter[?]] = arrayFilters,
  ): UpdateOptions =
    new UpdateOptions(upsert, arrayFilters)

object UpdateOptions:
  val default: UpdateOptions = new UpdateOptions(upsert = false, arrayFilters = Nil)
  val upsert: UpdateOptions  = default.withUpsert
