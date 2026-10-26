package mongo4s.operations

final class SearchOptions private (
    val index: String,
    val count: Option[SearchCount],
    val returnStoredSource: Boolean,
):
  def withIndex(name: String): SearchOptions = copy(index = name)

  def withCount(value: SearchCount): SearchOptions = copy(count = Some(value))

  def withReturnStoredSource: SearchOptions = copy(returnStoredSource = true)

  private def copy(
      index: String = index,
      count: Option[SearchCount] = count,
      returnStoredSource: Boolean = returnStoredSource,
  ): SearchOptions =
    new SearchOptions(
      index = index,
      count = count,
      returnStoredSource = returnStoredSource,
    )

object SearchOptions:
  val DefaultIndex = "default"

  val default: SearchOptions =
    new SearchOptions(
      index = DefaultIndex,
      count = None,
      returnStoredSource = false,
    )

enum SearchCount:
  case Total
  case LowerBound(threshold: Option[Int])

object SearchCount:
  val total: SearchCount      = Total
  val lowerBound: SearchCount = LowerBound(None)

  def lowerBound(threshold: Int): SearchCount =
    require(threshold > 0, s"a lower-bound count threshold must be positive, got $threshold")
    LowerBound(Some(threshold))
