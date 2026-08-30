package mongo4s.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.bson.{BsonError, DecodeResult}

private[mongo4s] final class AttemptingPublisher[Src, A](
    source: Publisher[Src],
    decode: Src => DecodeResult[A],
) extends Publisher[DecodeResult[A]]:

  def subscribe(downstream: Subscriber[? >: DecodeResult[A]]): Unit =
    source.subscribe(
      new Subscriber[Src]:
        private val terminated = AtomicBoolean(false)

        def onSubscribe(subscription: Subscription): Unit = downstream.onSubscribe(subscription)

        def onNext(value: Src): Unit =
          if !terminated.get then
            val decoded =
              try decode(value)
              catch case NonFatal(error) => Left(BsonError.fromThrowable(error))

            downstream.onNext(decoded)

        def onError(error: Throwable): Unit =
          if terminated.compareAndSet(false, true) then downstream.onError(error)

        def onComplete(): Unit =
          if terminated.compareAndSet(false, true) then downstream.onComplete()
    )
