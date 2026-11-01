package mongo4s.internal

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.ExplainVerbosity
import com.mongodb.client.model.Collation
import com.mongodb.reactivestreams.client.{AggregatePublisher, ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.{BsonDocumentDecoder, DecodeResult, FieldNaming}
import mongo4s.operations.Stage
import mongo4s.queries.{AggregateQuery, DecodeAttempts}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class AggregateQueryImpl[F[*], S[*], A](
    collection: RSMongoCollection[BsonDocument],
    pipeline: Seq[Stage[?]],
    naming: FieldNaming,
    decoder: BsonDocumentDecoder[A],
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

  def let(variables: BsonDocument): AggregateQuery[F, S, A]             = copy(options = options.withLet(variables))
  def maxAwaitTime(duration: FiniteDuration): AggregateQuery[F, S, A]   = copy(options = options.withMaxAwaitTime(duration))
  def bypassDocumentValidation(value: Boolean): AggregateQuery[F, S, A] = copy(options = options.withBypassDocumentValidation(value))
  def explain(verbosity: ExplainVerbosity): F[BsonDocument]             =
    rs.one(documents(limited = false).explain(classOf[BsonDocument], verbosity))

  def first: F[Option[A]]                  = rs.option(publisher(limited = true))
  def all: F[List[A]]                      = rs.list(publisher(limited = false))
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher(limited = false))

  def attempting: DecodeAttempts[F, S, A] = new DecodeAttempts[F, S, A]:
    def all: F[List[DecodeResult[A]]] =
      rs.list(AttemptingPublisher(documents(limited = false), decoder.decodeDocument))

    def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]] =
      rs.stream(AttemptingPublisher(documents(limited = false), decoder.decodeDocument))

  private def publisher(limited: Boolean): Publisher[A] = DecodingPublisher(documents(limited), decoder.decodeDocument)

  private def documents(limited: Boolean): AggregatePublisher[BsonDocument] =
    val stages = AggregateQueryImpl.render(pipeline, naming, limited)

    val base: AggregatePublisher[BsonDocument] =
      session match
        case Some(s) => collection.aggregate(s, stages.asJava, classOf[BsonDocument])
        case None    => collection.aggregate(stages.asJava, classOf[BsonDocument])

    var aggregate = base
    allowDiskUse.foreach(allow => aggregate = aggregate.allowDiskUse(allow))
    options.hint.foreach(keys => aggregate = aggregate.hint(keys))
    options.collation.foreach(value => aggregate = aggregate.collation(value))
    options.maxTimeMillis.foreach(millis => aggregate = aggregate.maxTime(millis, QueryOptions.MillisUnit))
    options.batchSize.foreach(n => aggregate = aggregate.batchSize(n))
    options.comment.foreach(value => aggregate = aggregate.comment(value))
    options.let.foreach(value => aggregate = aggregate.let(value))
    options.maxAwaitTimeMillis.foreach(millis => aggregate = aggregate.maxAwaitTime(millis, QueryOptions.MillisUnit))
    options.bypassDocumentValidation.foreach(value => aggregate = aggregate.bypassDocumentValidation(value))

    aggregate
  end documents

  private def copy(
      allowDiskUse: Option[Boolean] = allowDiskUse,
      options: QueryOptions = options,
  ): AggregateQueryImpl[F, S, A] =
    AggregateQueryImpl(collection, pipeline, naming, decoder, allowDiskUse, session, options)

private[mongo4s] object AggregateQueryImpl:
  private val TerminalStages = Set("$out", "$merge")

  private[internal] def render(pipeline: Seq[Stage[?]], naming: FieldNaming, limited: Boolean): Seq[BsonDocument] =
    val stages = pipeline.map(_.toBson(naming))

    if limited && !stages.lastOption.exists(_.keySet.asScala.exists(TerminalStages.contains))
    then stages :+ Stage.Limit[Any](1).toBson(FieldNaming.identity)
    else stages
