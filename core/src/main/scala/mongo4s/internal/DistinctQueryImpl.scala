package mongo4s.internal

import scala.concurrent.duration.FiniteDuration

import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonValue}
import org.reactivestreams.Publisher
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.{ClientSession, DistinctPublisher, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.{BsonDecoder, DecodeResult}
import mongo4s.queries.{DecodeAttempts, DistinctQuery}

private[mongo4s] final class DistinctQueryImpl[F[*], S[*], A](
    collection: RSMongoCollection[BsonDocument],
    field: String,
    filter: Bson,
    decoder: BsonDecoder[A],
    session: Option[ClientSession],
    options: QueryOptions = QueryOptions.empty,
)(using rs: RsBridge[F, S])
    extends DistinctQuery[F, S, A]:

  def collation(value: Collation): DistinctQuery[F, S, A]       = copy(options = options.withCollation(value))
  def maxTime(duration: FiniteDuration): DistinctQuery[F, S, A] = copy(options = options.withMaxTime(duration))
  def batchSize(n: Int): DistinctQuery[F, S, A]                 = copy(options = options.withBatchSize(n))

  def first: F[Option[A]]                  = rs.option(publisher)
  def all: F[List[A]]                      = rs.list(publisher)
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher)

  def attempting: DecodeAttempts[F, S, A] = new DecodeAttempts[F, S, A]:
    def all: F[List[DecodeResult[A]]]                                    = rs.list(AttemptingPublisher(values, decoder.decode))
    def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]] = rs.stream(AttemptingPublisher(values, decoder.decode))
  end attempting

  private def publisher: Publisher[A] = DecodingPublisher(values, decoder.decode)

  private def values: DistinctPublisher[BsonValue] =
    val base: DistinctPublisher[BsonValue] = session match
      case Some(s) => collection.distinct(s, field, filter, classOf[BsonValue])
      case None    => collection.distinct(field, filter, classOf[BsonValue])

    var distinct = base
    options.collation.foreach(value => distinct = distinct.collation(value))
    options.maxTimeMillis.foreach(millis => distinct = distinct.maxTime(millis, QueryOptions.MillisUnit))
    options.batchSize.foreach(n => distinct = distinct.batchSize(n))

    distinct
  end values

  private def copy(options: QueryOptions): DistinctQueryImpl[F, S, A] =
    DistinctQueryImpl(
      collection = collection,
      field = field,
      filter = filter,
      decoder = decoder,
      session = session,
      options = options,
    )
