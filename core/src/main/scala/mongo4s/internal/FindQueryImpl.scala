package mongo4s.internal

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.{CursorType, ExplainVerbosity}
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.{ClientSession, FindPublisher, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.operations.{Bounds, Filter, Projection, Sort}
import mongo4s.queries.{DecodeAttempts, FindQuery, SelectQuery}
import mongo4s.bson.{BsonDocumentCodec, BsonDocumentDecoder, DecodeResult, FieldNaming}

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
  def skip(n: Int): FindQuery[F, S, A]                     = copy(skip = Some(Bounds.notNegative(n, "skip")))
  def limit(n: Int): FindQuery[F, S, A]                    = copy(limit = Some(Bounds.atLeastOne(n, "limit", Bounds.NoLimit)))

  def hint(keys: BsonDocument): FindQuery[F, S, A]          = copy(options = options.withHint(keys))
  def collation(value: Collation): FindQuery[F, S, A]       = copy(options = options.withCollation(value))
  def maxTime(duration: FiniteDuration): FindQuery[F, S, A] = copy(options = options.withMaxTime(duration))
  def batchSize(n: Int): FindQuery[F, S, A]                 = copy(options = options.withBatchSize(n))
  def comment(value: String): FindQuery[F, S, A]            = copy(options = options.withComment(value))

  def let(variables: BsonDocument): FindQuery[F, S, A]           = copy(options = options.withLet(variables))
  def allowDiskUse(allow: Boolean): FindQuery[F, S, A]           = copy(options = options.withAllowDiskUse(allow))
  def cursorType(value: CursorType): FindQuery[F, S, A]          = copy(options = options.withCursorType(value))
  def maxAwaitTime(duration: FiniteDuration): FindQuery[F, S, A] = copy(options = options.withMaxAwaitTime(duration))
  def noCursorTimeout: FindQuery[F, S, A]                        = copy(options = options.withNoCursorTimeout)

  def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
    rs.one(documents(limit).explain(classOf[BsonDocument], verbosity))

  def first: F[Option[A]]                  = rs.option(publisher(Some(1)))
  def all: F[List[A]]                      = rs.list(publisher(limit))
  def stream(using Streamable[S, A]): S[A] = streamed(publisher(limit))

  def selecting[B](selected: Projection[A], decoder: FieldNaming => BsonDocumentDecoder[B]): SelectQuery[F, S, B] =
    SelectQueryImpl(documents(_, selected), limit, decoder(naming), options.tailable)

  private def streamed[B](publisher: => Publisher[B])(using Streamable[S, B]): S[B] =
    if options.tailable
    then rs.liveStream(publisher)
    else rs.stream(publisher)

  def attempting: DecodeAttempts[F, S, A] = new DecodeAttempts[F, S, A]:
    def all: F[List[DecodeResult[A]]]                                    = rs.list(attemptingPublisher(limit))
    def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]] = streamed(attemptingPublisher(limit))
  end attempting

  private def documents(effectiveLimit: Option[Int], selected: Projection[A] = projection): FindPublisher[BsonDocument] =
    val base: FindPublisher[BsonDocument] = session match
      case Some(s) => collection.find(s, filter.toBson(naming))
      case None    => collection.find(filter.toBson(naming))

    var find = base
    if !sort.isEmpty
    then find = find.sort(sort.toBson(naming))
    if !selected.isEmpty
    then find = find.projection(selected.toBson(naming))

    skip.foreach(n => find = find.skip(n))
    effectiveLimit.foreach(n => find = find.limit(n))

    options.hint.foreach(keys => find = find.hint(keys))
    options.collation.foreach(value => find = find.collation(value))
    options.maxTimeMillis.foreach(millis => find = find.maxTime(millis, QueryOptions.MillisUnit))
    options.batchSize.foreach(n => find = find.batchSize(n))
    options.comment.foreach(value => find = find.comment(value))
    options.let.foreach(value => find = find.let(value))
    options.allowDiskUse.foreach(allow => find = find.allowDiskUse(allow))
    options.cursorType.foreach(value => find = find.cursorType(value))
    options.maxAwaitTimeMillis.foreach(millis => find = find.maxAwaitTime(millis, QueryOptions.MillisUnit))
    if options.noCursorTimeout then find = find.noCursorTimeout(true)

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
