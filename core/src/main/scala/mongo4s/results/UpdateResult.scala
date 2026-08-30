package mongo4s.results

import org.bson.BsonValue

final case class UpdateResult(
    matchedCount: Long,
    modifiedCount: Long,
    upsertedId: Option[BsonValue],
):
  def wasUpserted: Boolean = upsertedId.isDefined
  def wasApplied: Boolean  = matchedCount > 0 || wasUpserted

object UpdateResult:
  val none: UpdateResult =
    UpdateResult(
      matchedCount = 0,
      modifiedCount = 0,
      upsertedId = None,
    )
