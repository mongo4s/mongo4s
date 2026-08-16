package mongo4s.internal

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.bson.BsonError

private[mongo4s] final class DecodingPublisher[Src, A](
    source: Publisher[Src],
    decode: Src => Either[BsonError, A],
) extends Publisher[A]:

  def subscribe(downstream: Subscriber[? >: A]): Unit =
    source.subscribe(
      new Subscriber[Src]:
        private var subscription: Subscription = scala.compiletime.uninitialized

        def onSubscribe(s: Subscription): Unit =
          subscription = s
          downstream.onSubscribe(s)

        def onNext(value: Src): Unit =
          val decoded =
            try decode(value)
            catch case error: Throwable => Left(BsonError.fromThrowable(error))
          decoded match
            case Right(value) => downstream.onNext(value)
            case Left(error)  =>
              subscription.cancel()
              downstream.onError(error.toThrowable)

        def onError(error: Throwable): Unit = downstream.onError(error)
        def onComplete(): Unit              = downstream.onComplete()
    )
