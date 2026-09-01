package mongo4s.operations

final class GraphLookupOptions[B] private (
    val maxDepth: Option[Int],
    val depthField: Option[String],
    val restrictSearch: Option[Filter[B]],
):
  def withMaxDepth(value: Int): GraphLookupOptions[B] = copy(maxDepth = Some(value))

  def withDepthField(value: String): GraphLookupOptions[B] = copy(depthField = Some(value))

  def restrictedTo(filter: Filter[B]): GraphLookupOptions[B] = copy(restrictSearch = Some(filter))

  def isEmpty: Boolean = maxDepth.isEmpty && depthField.isEmpty && restrictSearch.isEmpty

  private def copy(
      maxDepth: Option[Int] = maxDepth,
      depthField: Option[String] = depthField,
      restrictSearch: Option[Filter[B]] = restrictSearch,
  ): GraphLookupOptions[B] =
    new GraphLookupOptions(maxDepth, depthField, restrictSearch)

object GraphLookupOptions:
  def default[B]: GraphLookupOptions[B] =
    new GraphLookupOptions(maxDepth = None, depthField = None, restrictSearch = None)
