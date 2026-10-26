package mongo4s.results

import com.mongodb.client.model.bulk.ClientBulkWriteResult as DriverClientBulkWriteResult

final case class ClientBulkWriteResult(
    insertedCount: Long,
    matchedCount: Long,
    modifiedCount: Long,
    deletedCount: Long,
    upsertedCount: Long,
)

object ClientBulkWriteResult:
  val none: ClientBulkWriteResult = ClientBulkWriteResult(0, 0, 0, 0, 0)

  def fromDriver(result: DriverClientBulkWriteResult): ClientBulkWriteResult =
    if result.isAcknowledged
    then
      ClientBulkWriteResult(
        insertedCount = result.getInsertedCount,
        matchedCount = result.getMatchedCount,
        modifiedCount = result.getModifiedCount,
        deletedCount = result.getDeletedCount,
        upsertedCount = result.getUpsertedCount,
      )
    else none
