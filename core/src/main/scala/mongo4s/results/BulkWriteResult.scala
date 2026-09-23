package mongo4s.results

import org.bson.BsonValue
import com.mongodb.bulk.{BulkWriteUpsert, BulkWriteResult as DriverBulkWriteResult}

import scala.jdk.CollectionConverters.given

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

  private[mongo4s] def toDriver: DriverBulkWriteResult =
    DriverBulkWriteResult.acknowledged(
      insertedCount.toInt,
      matchedCount.toInt,
      deletedCount.toInt,
      Integer.valueOf(modifiedCount.toInt),
      upsertedIds.toList.sortBy(_._1).map((index, id) => BulkWriteUpsert(index, id)).asJava,
      java.util.List.of(),
    )

object BulkWriteResult:
  val none: BulkWriteResult =
    BulkWriteResult(
      insertedCount = 0,
      matchedCount = 0,
      modifiedCount = 0,
      deletedCount = 0,
      upsertedIds = Map.empty,
    )

  def fromDriver(result: DriverBulkWriteResult): BulkWriteResult =
    if result.wasAcknowledged
    then
      BulkWriteResult(
        insertedCount = result.getInsertedCount.toLong,
        matchedCount = result.getMatchedCount.toLong,
        modifiedCount = result.getModifiedCount.toLong,
        deletedCount = result.getDeletedCount.toLong,
        upsertedIds = result.getUpserts.asScala.map(upsert => upsert.getIndex -> upsert.getId).toMap,
      )
    else none

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
