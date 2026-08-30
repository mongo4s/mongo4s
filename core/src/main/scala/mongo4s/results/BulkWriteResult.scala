package mongo4s.results

import org.bson.BsonValue

final case class BulkWriteResult(
    insertedCount: Long,
    matchedCount: Long,
    modifiedCount: Long,
    deletedCount: Long,
    upsertedIds: Map[Int, BsonValue],
):

  def shiftUpsertedIds(by: Int): BulkWriteResult =
    if by == 0 || upsertedIds.isEmpty
    then this
    else copy(upsertedIds = upsertedIds.map((index, id) => (index + by, id)))

object BulkWriteResult:
  val none: BulkWriteResult =
    BulkWriteResult(
      insertedCount = 0,
      matchedCount = 0,
      modifiedCount = 0,
      deletedCount = 0,
      upsertedIds = Map.empty,
    )

  def combine(results: Seq[BulkWriteResult]): BulkWriteResult =
    results.foldLeft(none) { (acc, next) =>
      BulkWriteResult(
        insertedCount = acc.insertedCount + next.insertedCount,
        matchedCount = acc.matchedCount + next.matchedCount,
        modifiedCount = acc.modifiedCount + next.modifiedCount,
        deletedCount = acc.deletedCount + next.deletedCount,
        upsertedIds = acc.upsertedIds ++ next.upsertedIds,
      )
    }
