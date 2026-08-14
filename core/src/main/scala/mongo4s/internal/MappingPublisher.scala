package mongo4s.internal

import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo4s] final class MappingPublisher[A, B](source: Publisher[A], f: A => B) extends Publisher[B]:

  def subscribe(downstream: Subscriber[? >: B]): Unit =
    source.subscribe(
      new Subscriber[A]:
        def onSubscribe(subscription: Subscription): Unit = downstream.onSubscribe(subscription)
        def onNext(value: A): Unit                        = downstream.onNext(f(value))
        def onError(error: Throwable): Unit               = downstream.onError(error)
        def onComplete(): Unit                            = downstream.onComplete()
    )
