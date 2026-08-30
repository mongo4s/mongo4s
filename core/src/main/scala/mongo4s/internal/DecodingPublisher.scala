package mongo4s.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

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

        private val terminated = AtomicBoolean(false)

        def onSubscribe(s: Subscription): Unit =
          subscription = s
          downstream.onSubscribe(s)
        end onSubscribe

        def onNext(value: Src): Unit =
          if !terminated.get
          then
            val decoded =
              try decode(value)
              catch case NonFatal(error) => Left(BsonError.fromThrowable(error))

            decoded match
              case Right(decodedValue) => downstream.onNext(decodedValue)
              case Left(error)         =>
                if terminated.compareAndSet(false, true) then
                  subscription.cancel()
                  downstream.onError(error.toThrowable)
        end onNext

        def onError(error: Throwable): Unit =
          if terminated.compareAndSet(false, true)
          then downstream.onError(error)

        def onComplete(): Unit =
          if terminated.compareAndSet(false, true)
          then downstream.onComplete()
    )
