package mongo4s.internal

import java.util.concurrent.CompletableFuture

import scala.collection.mutable.ListBuffer

import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo4s] object PublisherCollector:
  def collect[A](publisher: Publisher[A]): CompletableFuture[List[A]] =
    val future = CompletableFuture[List[A]]()
    val buffer = ListBuffer.empty[A]
    publisher.subscribe(
      new Subscriber[A]:
        def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue)
        def onNext(value: A): Unit                        = buffer += value
        def onError(error: Throwable): Unit               = future.completeExceptionally(error)
        def onComplete(): Unit                            = future.complete(buffer.toList)
    )
    future
