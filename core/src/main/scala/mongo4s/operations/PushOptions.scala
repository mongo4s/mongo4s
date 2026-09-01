package mongo4s.operations

final class PushOptions[A] private (
    val slice: Option[Int],
    val sort: Option[Sort[A]],
    val sortScalars: Option[Boolean],
    val position: Option[Int],
):
  def withSlice(value: Int): PushOptions[A] = copy(slice = Some(value))

  def withPosition(value: Int): PushOptions[A] = copy(position = Some(value))

  def sortedBy(value: Sort[A]): PushOptions[A] = copy(sort = Some(value), sortScalars = None)

  def sortedAscending: PushOptions[A] = copy(sort = None, sortScalars = Some(true))

  def sortedDescending: PushOptions[A] = copy(sort = None, sortScalars = Some(false))

  private def copy(
      slice: Option[Int] = slice,
      sort: Option[Sort[A]] = sort,
      sortScalars: Option[Boolean] = sortScalars,
      position: Option[Int] = position,
  ): PushOptions[A] =
    new PushOptions(slice, sort, sortScalars, position)

object PushOptions:
  def default[A]: PushOptions[A] =
    new PushOptions(slice = None, sort = None, sortScalars = None, position = None)
