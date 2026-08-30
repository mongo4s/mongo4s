package mongo4s.internal

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.{ClientSession, FindPublisher, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.queries.{DecodeAttempts, FindQuery}
import mongo4s.operations.{Filter, Projection, Sort}
import mongo4s.bson.{BsonDocumentCodec, DecodeResult, FieldNaming}

private[mongo4s] final class FindQueryImpl[F[*], S[*], A](
    collection: RSMongoCollection[BsonDocument],
    naming: FieldNaming,
    codec: BsonDocumentCodec[A],
    filter: Filter[A],
    sort: Sort[A],
    projection: Projection[A],
    skip: Option[Int],
    limit: Option[Int],
    session: Option[ClientSession],
    options: QueryOptions = QueryOptions.empty,
)(using rs: RsBridge[F, S])
    extends FindQuery[F, S, A]:

  def filter(value: Filter[A]): FindQuery[F, S, A]         = copy(filter = Filter.and(filter, value))
  def sort(value: Sort[A]): FindQuery[F, S, A]             = copy(sort = value)
  def projection(value: Projection[A]): FindQuery[F, S, A] = copy(projection = value)
  def skip(n: Int): FindQuery[F, S, A]                     = copy(skip = Some(n))
  def limit(n: Int): FindQuery[F, S, A]                    = copy(limit = Some(n))

  def hint(keys: BsonDocument): FindQuery[F, S, A]          = copy(options = options.withHint(keys))
  def collation(value: Collation): FindQuery[F, S, A]       = copy(options = options.withCollation(value))
  def maxTime(duration: FiniteDuration): FindQuery[F, S, A] = copy(options = options.withMaxTime(duration))
  def batchSize(n: Int): FindQuery[F, S, A]                 = copy(options = options.withBatchSize(n))
  def comment(value: String): FindQuery[F, S, A]            = copy(options = options.withComment(value))

  def first: F[Option[A]]                  = rs.option(publisher(Some(1)))
  def all: F[List[A]]                      = rs.list(publisher(limit))
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher(limit))

  def attempting: DecodeAttempts[F, S, A] = new DecodeAttempts[F, S, A]:
    def all: F[List[DecodeResult[A]]]                                    = rs.list(attemptingPublisher(limit))
    def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]] = rs.stream(attemptingPublisher(limit))
  end attempting

  private def documents(effectiveLimit: Option[Int]): FindPublisher[BsonDocument] =
    val base: FindPublisher[BsonDocument] = session match
      case Some(s) => collection.find(s, filter.toBson(naming))
      case None    => collection.find(filter.toBson(naming))

    var find = base
    if !sort.isEmpty
    then find = find.sort(sort.toBson(naming))
    if !projection.isEmpty
    then find = find.projection(projection.toBson(naming))
    skip.foreach(n => find = find.skip(n))
    effectiveLimit.foreach(n => find = find.limit(n))

    options.hint.foreach(keys => find = find.hint(keys))
    options.collation.foreach(value => find = find.collation(value))
    options.maxTimeMillis.foreach(millis => find = find.maxTime(millis, QueryOptions.MillisUnit))
    options.batchSize.foreach(n => find = find.batchSize(n))
    options.comment.foreach(value => find = find.comment(value))

    find
  end documents

  private def publisher(effectiveLimit: Option[Int]): Publisher[A] =
    DecodingPublisher(
      documents(effectiveLimit),
      codec.decodeDocument,
    )

  private def attemptingPublisher(effectiveLimit: Option[Int]): Publisher[DecodeResult[A]] =
    AttemptingPublisher(
      documents(effectiveLimit),
      codec.decodeDocument,
    )

  private def copy(
      filter: Filter[A] = filter,
      sort: Sort[A] = sort,
      projection: Projection[A] = projection,
      skip: Option[Int] = skip,
      limit: Option[Int] = limit,
      options: QueryOptions = options,
  ): FindQueryImpl[F, S, A] =
    FindQueryImpl(
      collection = collection,
      naming = naming,
      codec = codec,
      filter = filter,
      sort = sort,
      projection = projection,
      skip = skip,
      limit = limit,
      session = session,
      options = options,
    )
