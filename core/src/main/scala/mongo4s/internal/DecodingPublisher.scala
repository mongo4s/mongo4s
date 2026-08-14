package mongo4s.internal

import org.bson.BsonDocument
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.bson.BsonError

private[mongo4s] final class DecodingPublisher[A](
    source: Publisher[BsonDocument],
    decode: BsonDocument => Either[BsonError, A],
) extends Publisher[A]:

  def subscribe(downstream: Subscriber[? >: A]): Unit =
    source.subscribe(
      new Subscriber[BsonDocument]:
        private var subscription: Subscription = scala.compiletime.uninitialized

        def onSubscribe(s: Subscription): Unit =
          subscription = s
          downstream.onSubscribe(s)

        def onNext(document: BsonDocument): Unit =
          val decoded =
            try decode(document)
            catch case error: Throwable => Left(BsonError.fromThrowable(error))
          decoded match
            case Right(value) => downstream.onNext(value)
            case Left(error)  =>
              subscription.cancel()
              downstream.onError(error.toThrowable)

        def onError(error: Throwable): Unit = downstream.onError(error)
        def onComplete(): Unit              = downstream.onComplete()
    )
