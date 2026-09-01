package mongo4s.operations

final class FindOneAndUpdateOptions[E] private (
    val returnUpdated: Boolean,
    val upsert: Boolean,
    val sort: Sort[E],
    val projection: Projection[E],
    val arrayFilters: Seq[Filter[?]],
):
  def returningPrevious: FindOneAndUpdateOptions[E] = copy(returnUpdated = false)

  def withUpsert: FindOneAndUpdateOptions[E] = copy(upsert = true)

  def withSort(value: Sort[E]): FindOneAndUpdateOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndUpdateOptions[E] = copy(projection = value)

  def withArrayFilters(filters: Seq[Filter[?]]): FindOneAndUpdateOptions[E] = copy(arrayFilters = filters)

  private def copy(
      returnUpdated: Boolean = returnUpdated,
      upsert: Boolean = upsert,
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
      arrayFilters: Seq[Filter[?]] = arrayFilters,
  ): FindOneAndUpdateOptions[E] =
    new FindOneAndUpdateOptions(returnUpdated, upsert, sort, projection, arrayFilters)

object FindOneAndUpdateOptions:
  def default[E]: FindOneAndUpdateOptions[E] =
    new FindOneAndUpdateOptions(
      returnUpdated = true,
      upsert = false,
      sort = Sort.empty[E],
      projection = Projection.empty[E],
      arrayFilters = Nil,
    )

  def upsert[E]: FindOneAndUpdateOptions[E] = default[E].withUpsert
