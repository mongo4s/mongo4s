package mongo4s.operations

final class FindOneAndReplaceOptions[E] private (
    val returnUpdated: Boolean,
    val upsert: Boolean,
    val sort: Sort[E],
    val projection: Projection[E],
):
  def returningPrevious: FindOneAndReplaceOptions[E] = copy(returnUpdated = false)

  def withUpsert: FindOneAndReplaceOptions[E] = copy(upsert = true)

  def withSort(value: Sort[E]): FindOneAndReplaceOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndReplaceOptions[E] = copy(projection = value)

  private def copy(
      returnUpdated: Boolean = returnUpdated,
      upsert: Boolean = upsert,
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
  ): FindOneAndReplaceOptions[E] =
    new FindOneAndReplaceOptions(returnUpdated, upsert, sort, projection)

object FindOneAndReplaceOptions:
  def default[E]: FindOneAndReplaceOptions[E] =
    new FindOneAndReplaceOptions(
      returnUpdated = true,
      upsert = false,
      sort = Sort.empty[E],
      projection = Projection.empty[E],
    )

  def upsert[E]: FindOneAndReplaceOptions[E] = default[E].withUpsert
