package mongo4s.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.CursorType
import com.mongodb.client.model.Collation

import mongo4s.operations.{Bounds, TimeLimit}

private[mongo4s] final case class QueryOptions(
    hint: Option[BsonDocument] = None,
    collation: Option[Collation] = None,
    maxTime: Option[FiniteDuration] = None,
    batchSize: Option[Int] = None,
    comment: Option[String] = None,
    let: Option[BsonDocument] = None,
    allowDiskUse: Option[Boolean] = None,
    maxAwaitTime: Option[FiniteDuration] = None,
    cursorType: Option[CursorType] = None,
    noCursorTimeout: Boolean = false,
    bypassDocumentValidation: Option[Boolean] = None,
):
  def withHint(keys: BsonDocument): QueryOptions                 = copy(hint = Some(keys))
  def withCollation(value: Collation): QueryOptions              = copy(collation = Some(value))
  def withMaxTime(value: FiniteDuration): QueryOptions           = copy(maxTime = Some(TimeLimit.millis(value, "maxTime")))
  def withBatchSize(value: Int): QueryOptions                    = copy(batchSize = Some(Bounds.atLeastOne(value, "batchSize", Bounds.NoBatching)))
  def withComment(value: String): QueryOptions                   = copy(comment = Some(value))
  def withLet(variables: BsonDocument): QueryOptions             = copy(let = Some(variables))
  def withAllowDiskUse(allow: Boolean): QueryOptions             = copy(allowDiskUse = Some(allow))
  def withMaxAwaitTime(value: FiniteDuration): QueryOptions      = copy(maxAwaitTime = Some(TimeLimit.millis(value, "maxAwaitTime")))
  def withCursorType(value: CursorType): QueryOptions            = copy(cursorType = Some(value))
  def withNoCursorTimeout: QueryOptions                          = copy(noCursorTimeout = true)
  def withBypassDocumentValidation(value: Boolean): QueryOptions = copy(bypassDocumentValidation = Some(value))

  def maxTimeMillis: Option[Long]      = maxTime.map(_.toMillis)
  def maxAwaitTimeMillis: Option[Long] = maxAwaitTime.map(_.toMillis)

  def tailable: Boolean = cursorType.exists(_ != CursorType.NonTailable)

private[mongo4s] object QueryOptions:
  val empty: QueryOptions = QueryOptions()

  val MillisUnit: TimeUnit = TimeUnit.MILLISECONDS
