package mongo4s.operations

final class GeoNearOptions[E] private (
    val query: Option[Filter[E]],
    val minDistance: Option[Double],
    val maxDistance: Option[Double],
    val distanceMultiplier: Option[Double],
    val includeLocs: Option[String],
):
  def where(filter: Filter[E]): GeoNearOptions[E] = copy(query = Some(filter))

  def withMinDistance(meters: Double): GeoNearOptions[E] = copy(minDistance = Some(Bounds.notNegative(meters, "$geoNear minDistance")))
  def withMaxDistance(meters: Double): GeoNearOptions[E] = copy(maxDistance = Some(Bounds.notNegative(meters, "$geoNear maxDistance")))

  def withDistanceMultiplier(value: Double): GeoNearOptions[E] =
    copy(distanceMultiplier = Some(Bounds.notNegative(value, "$geoNear distanceMultiplier")))

  def withIncludeLocs(field: String): GeoNearOptions[E] = copy(includeLocs = Some(field))

  private def copy(
      query: Option[Filter[E]] = query,
      minDistance: Option[Double] = minDistance,
      maxDistance: Option[Double] = maxDistance,
      distanceMultiplier: Option[Double] = distanceMultiplier,
      includeLocs: Option[String] = includeLocs,
  ): GeoNearOptions[E] =
    new GeoNearOptions(query, minDistance, maxDistance, distanceMultiplier, includeLocs)

object GeoNearOptions:
  def default[E]: GeoNearOptions[E] =
    new GeoNearOptions(
      query = None,
      minDistance = None,
      maxDistance = None,
      distanceMultiplier = None,
      includeLocs = None,
    )
