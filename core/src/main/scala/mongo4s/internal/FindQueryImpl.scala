package mongo4s.internal

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.queries.FindQuery
import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.{BsonDocumentCodec, FieldNaming}
import mongo4s.operations.{Filter, Projection, Sort}

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
)(using rs: RsBridge[F, S])
    extends FindQuery[F, S, A]:

  def filter(value: Filter[A]): FindQuery[F, S, A]         = copy(filter = Filter.and(filter, value))
  def sort(value: Sort[A]): FindQuery[F, S, A]             = copy(sort = value)
  def projection(value: Projection[A]): FindQuery[F, S, A] = copy(projection = value)
  def skip(n: Int): FindQuery[F, S, A]                     = copy(skip = Some(n))
  def limit(n: Int): FindQuery[F, S, A]                    = copy(limit = Some(n))

  def first: F[Option[A]]                  = rs.option(publisher(Some(1)))
  def all: F[List[A]]                      = rs.list(publisher(limit))
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher(limit))

  private def publisher(effectiveLimit: Option[Int]): Publisher[A] =
    val find = session match
      case Some(s) => collection.find(s, filter.toBson(naming))
      case None    => collection.find(filter.toBson(naming))
    if !sort.isEmpty then find.sort(sort.toBson(naming))
    if !projection.isEmpty then find.projection(projection.toBson(naming))
    skip.foreach(find.skip)
    effectiveLimit.foreach(find.limit)
    DecodingPublisher(find, codec.decodeDocument)

  private def copy(
      filter: Filter[A] = filter,
      sort: Sort[A] = sort,
      projection: Projection[A] = projection,
      skip: Option[Int] = skip,
      limit: Option[Int] = limit,
  ): FindQueryImpl[F, S, A] =
    FindQueryImpl(collection, naming, codec, filter, sort, projection, skip, limit, session)
