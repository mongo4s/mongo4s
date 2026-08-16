package mongo4s.internal

import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.reactivestreams.Publisher
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.BsonDocumentCodec
import mongo4s.queries.AggregateQuery

import scala.jdk.CollectionConverters.given

private[mongo4s] final class AggregateQueryImpl[F[*], S[*], A](
    collection: RSMongoCollection[BsonDocument],
    pipeline: Seq[Bson],
    codec: BsonDocumentCodec[A],
    allowDiskUse: Option[Boolean],
    session: Option[ClientSession],
)(using rs: RsBridge[F, S])
    extends AggregateQuery[F, S, A]:

  def allowDiskUse(allow: Boolean): AggregateQuery[F, S, A] =
    AggregateQueryImpl(collection, pipeline, codec, Some(allow), session)

  def first: F[Option[A]]                  = rs.option(publisher)
  def all: F[List[A]]                      = rs.list(publisher)
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher)

  private def publisher: Publisher[A] =
    val aggregate = session match
      case Some(s) => collection.aggregate(s, pipeline.asJava, classOf[BsonDocument])
      case None    => collection.aggregate(pipeline.asJava, classOf[BsonDocument])
    allowDiskUse.foreach(aggregate.allowDiskUse(_))
    DecodingPublisher(aggregate, codec.decodeDocument)
