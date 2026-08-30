package mongo4s.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.client.model.Collation

private[mongo4s] final case class QueryOptions(
    hint: Option[BsonDocument] = None,
    collation: Option[Collation] = None,
    maxTime: Option[FiniteDuration] = None,
    batchSize: Option[Int] = None,
    comment: Option[String] = None,
):
  def withHint(keys: BsonDocument): QueryOptions       = copy(hint = Some(keys))
  def withCollation(value: Collation): QueryOptions    = copy(collation = Some(value))
  def withMaxTime(value: FiniteDuration): QueryOptions = copy(maxTime = Some(value))
  def withBatchSize(value: Int): QueryOptions          = copy(batchSize = Some(value))
  def withComment(value: String): QueryOptions         = copy(comment = Some(value))

  def maxTimeMillis: Option[Long] = maxTime.map(_.toMillis)

private[mongo4s] object QueryOptions:
  val empty: QueryOptions = QueryOptions()

  val MillisUnit: TimeUnit = TimeUnit.MILLISECONDS
