package mongo4s.internal

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable.ListBuffer

import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo4s] object PublisherCollector:

  def collect[A](publisher: Publisher[A], limit: Int = Int.MaxValue): CompletableFuture[List[A]] =
    subscribe(publisher, limit, buffered = true)

  def drain[A](publisher: Publisher[A]): CompletableFuture[Unit] =
    subscribe(
      publisher,
      Int.MaxValue,
      buffered = false,
    ).thenApply(_ => ())

  private def subscribe[A](publisher: Publisher[A], limit: Int, buffered: Boolean): CompletableFuture[List[A]] =
    val future          = CompletableFuture[List[A]]()
    val subscriptionRef = AtomicReference[Subscription]()
    val terminated      = AtomicBoolean(false)
    val buffer          = ListBuffer.empty[A]
    var received        = 0

    future.whenComplete: (_, _) =>
      val subscription = subscriptionRef.getAndSet(null)
      if subscription ne null then subscription.cancel()

    publisher.subscribe(
      new Subscriber[A]:
        def onSubscribe(subscription: Subscription): Unit =
          subscriptionRef.set(subscription)
          if future.isDone then subscription.cancel()
          else subscription.request(if limit == Int.MaxValue then Long.MaxValue else limit.toLong)

        def onNext(value: A): Unit =
          if !terminated.get
          then
            if buffered
            then buffer += value
            received += 1

            if received >= limit && terminated.compareAndSet(false, true)
            then future.complete(buffer.toList): Unit

        def onError(error: Throwable): Unit =
          if terminated.compareAndSet(false, true)
          then future.completeExceptionally(error): Unit

        def onComplete(): Unit =
          if terminated.compareAndSet(false, true)
          then future.complete(buffer.toList): Unit
    )

    future
  end subscribe
