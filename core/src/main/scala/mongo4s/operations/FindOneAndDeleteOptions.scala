package mongo4s.operations

final class FindOneAndDeleteOptions[E] private (
    val sort: Sort[E],
    val projection: Projection[E],
):
  def withSort(value: Sort[E]): FindOneAndDeleteOptions[E] = copy(sort = value)

  def withProjection(value: Projection[E]): FindOneAndDeleteOptions[E] = copy(projection = value)

  private def copy(
      sort: Sort[E] = sort,
      projection: Projection[E] = projection,
  ): FindOneAndDeleteOptions[E] =
    new FindOneAndDeleteOptions(sort, projection)

object FindOneAndDeleteOptions:
  def default[E]: FindOneAndDeleteOptions[E] =
    new FindOneAndDeleteOptions(sort = Sort.empty[E], projection = Projection.empty[E])
