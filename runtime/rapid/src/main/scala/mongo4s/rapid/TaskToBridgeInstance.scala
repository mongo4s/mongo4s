package mongo4s.rapid

import rapid.{Stream, Task}
import mongo4s.{RsBridge, Streamable}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import scala.collection.mutable.ListBuffer

trait TaskToBridgeInstance:

  given [A] => Streamable[RapidStream, A] = Streamable.instance

  // `list` bridges the Publisher directly through rapid's own `Task.completable` (a non-blocking,
  // callback-completed primitive - the rapid equivalent of a CompletableFuture/Promise) instead of
  // blocking a worker thread on a CountDownLatch inside `Task(...)`. Measured in mongo4s' runtime
  // benchmark (README "Runtime overhead"): the blocking-latch version cost roughly 4x throughput on
  // a ~100-document read versus this non-blocking version.
  given rapidBridge: RsBridge[Task, RapidStream] = new RsBridge[Task, RapidStream]:
    def one[A](publisher: => Publisher[A]): Task[A]                                        = list(publisher).map(_.head)
    def option[A](publisher: => Publisher[A]): Task[Option[A]]                             = list(publisher).map(_.headOption)
    def unit[A](publisher: => Publisher[A]): Task[Unit]                                    = list(publisher).map(_ => ())
    def stream[A](publisher: => Publisher[A])(using Streamable[RapidStream, A]): Stream[A] = Stream.force(list(publisher).map(Stream.emits))

    def list[A](publisher: => Publisher[A]): Task[List[A]] =
      Task.defer {
        val completable = Task.completable[List[A]]
        val buffer      = ListBuffer.empty[A]
        publisher.subscribe(
          new Subscriber[A]:
            def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue)
            def onNext(value: A): Unit                        = buffer += value
            def onError(error: Throwable): Unit               = completable.failure(error)
            def onComplete(): Unit                            = completable.success(buffer.toList)
        )
        completable
      }

object TaskToBridgeInstance extends TaskToBridgeInstance
