package mongo4s.results

import org.bson.BsonValue
import com.mongodb.client.result.UpdateResult as DriverUpdateResult

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

  def fromDriver(result: DriverUpdateResult): UpdateResult =
    if result.wasAcknowledged
    then
      UpdateResult(
        matchedCount = result.getMatchedCount,
        modifiedCount = result.getModifiedCount,
        upsertedId = Option(result.getUpsertedId),
      )
    else none
