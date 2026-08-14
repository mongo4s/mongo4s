package mongo4s.internal

import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonValue}
import com.mongodb.reactivestreams.client.MongoCollection as RSMongoCollection
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{RsBridge, Streamable}
import mongo4s.bson.{BsonDecoder, BsonError}
import mongo4s.queries.DistinctQuery

private[mongo4s] final class DistinctQueryImpl[F[*], S[*], A](
    collection: RSMongoCollection[BsonDocument],
    field: String,
    filter: Bson,
    decoder: BsonDecoder[A],
)(using rs: RsBridge[F, S])
    extends DistinctQuery[F, S, A]:

  def all: F[List[A]]                      = rs.list(publisher)
  def stream(using Streamable[S, A]): S[A] = rs.stream(publisher)

  private def publisher: Publisher[A] =
    val distinct = collection.distinct(field, filter, classOf[BsonValue])
    (downstream: Subscriber[? >: A]) =>
      distinct.subscribe(
        new Subscriber[BsonValue]:
          private var subscription: Subscription = scala.compiletime.uninitialized

          def onSubscribe(s: Subscription): Unit =
            subscription = s
            downstream.onSubscribe(s)

          def onNext(value: BsonValue): Unit =
            val decoded =
              try decoder.decode(value)
              catch case error: Throwable => Left(BsonError.fromThrowable(error))
            decoded match
              case Right(result) => downstream.onNext(result)
              case Left(error)   =>
                subscription.cancel()
                downstream.onError(error.toThrowable)

          def onError(error: Throwable): Unit = downstream.onError(error)

          def onComplete(): Unit = downstream.onComplete()
      )
