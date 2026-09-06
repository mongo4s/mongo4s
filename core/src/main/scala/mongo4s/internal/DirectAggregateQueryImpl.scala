package mongo4s.internal

import scala.concurrent.duration.FiniteDuration

import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonInt32}
import org.reactivestreams.Publisher
import com.mongodb.ExplainVerbosity
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.{AggregatePublisher, ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.{BsonDocumentCodec, DecodeResult}
import mongo4s.queries.{AggregateQuery, DecodeAttempts}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class DirectAggregateQueryImpl[F[*], S[*], A](
    typedCollection: RSMongoCollection[A],
    documentCollection: RSMongoCollection[BsonDocument],
    documentCodec: BsonDocumentCodec[A],
    pipeline: Seq[Bson],
    allowDiskUse: Option[Boolean],
    session: Option[ClientSession],
    options: QueryOptions = QueryOptions.empty,
)(using rs: RsBridge[F, S])
    extends AggregateQuery[F, S, A]:

  def allowDiskUse(allow: Boolean): AggregateQuery[F, S, A]      = copy(allowDiskUse = Some(allow))
  def hint(keys: BsonDocument): AggregateQuery[F, S, A]          = copy(options = options.withHint(keys))
  def collation(value: Collation): AggregateQuery[F, S, A]       = copy(options = options.withCollation(value))
  def maxTime(duration: FiniteDuration): AggregateQuery[F, S, A] = copy(options = options.withMaxTime(duration))
  def batchSize(n: Int): AggregateQuery[F, S, A]                 = copy(options = options.withBatchSize(n))
  def comment(value: String): AggregateQuery[F, S, A]            = copy(options = options.withComment(value))

  def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
    rs.one(documents(limited = false).explain(classOf[BsonDocument], verbosity))

  def first: F[Option[A]]                  = rs.option(typed(limited = true))
  def all: F[List[A]]                      = rs.list(typed(limited = false))
  def stream(using Streamable[S, A]): S[A] = rs.stream(typed(limited = false))

  def attempting: DecodeAttempts[F, S, A] = new DecodeAttempts[F, S, A]:
    def all: F[List[DecodeResult[A]]] =
      rs.list(AttemptingPublisher(documents(limited = false), documentCodec.decodeDocument))

    def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]] =
      rs.stream(AttemptingPublisher(documents(limited = false), documentCodec.decodeDocument))

  private def writesToCollection: Boolean =
    pipeline.lastOption match
      case Some(document: BsonDocument) => document.keySet.asScala.exists(AggregateQueryImpl.TerminalStages.contains)
      case _                            => false

  private def stagesFor(limited: Boolean): Seq[Bson] =
    if limited && !writesToCollection
    then pipeline :+ BsonDocument("$limit", BsonInt32(1))
    else pipeline

  private def typed(limited: Boolean): Publisher[A] =
    val stages = stagesFor(limited)

    val base: AggregatePublisher[A] =
      session match
        case Some(s) => typedCollection.aggregate(s, stages.asJava, typedCollection.getDocumentClass)
        case None    => typedCollection.aggregate(stages.asJava, typedCollection.getDocumentClass)

    configure(base)
  end typed

  private def documents(limited: Boolean): AggregatePublisher[BsonDocument] =
    val stages = stagesFor(limited)

    val base: AggregatePublisher[BsonDocument] =
      session match
        case Some(s) => documentCollection.aggregate(s, stages.asJava, classOf[BsonDocument])
        case None    => documentCollection.aggregate(stages.asJava, classOf[BsonDocument])

    configure(base)
  end documents

  private def configure[T](base: AggregatePublisher[T]): AggregatePublisher[T] =
    var aggregate = base
    allowDiskUse.foreach(allow => aggregate = aggregate.allowDiskUse(allow))
    options.hint.foreach(keys => aggregate = aggregate.hint(keys))
    options.collation.foreach(value => aggregate = aggregate.collation(value))
    options.maxTimeMillis.foreach(millis => aggregate = aggregate.maxTime(millis, QueryOptions.MillisUnit))
    options.batchSize.foreach(n => aggregate = aggregate.batchSize(n))
    options.comment.foreach(value => aggregate = aggregate.comment(value))
    aggregate
  end configure

  private def copy(
      allowDiskUse: Option[Boolean] = allowDiskUse,
      options: QueryOptions = options,
  ): DirectAggregateQueryImpl[F, S, A] =
    DirectAggregateQueryImpl(typedCollection, documentCollection, documentCodec, pipeline, allowDiskUse, session, options)
