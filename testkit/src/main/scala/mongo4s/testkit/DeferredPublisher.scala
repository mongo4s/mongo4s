package mongo4s.testkit

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.util.control.NonFatal

import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[testkit] final class DeferredPublisher[A](values: () => List[A]) extends Publisher[A]:

  def subscribe(subscriber: Subscriber[? >: A]): Unit =
    subscriber.onSubscribe(
      new Subscription:
        private val demand    = AtomicLong(0)
        private val draining  = AtomicInteger(0)
        private val done      = AtomicBoolean(false)
        private var remaining = Option.empty[Iterator[A]]

        def request(n: Long): Unit =
          if n <= 0
          then finish(subscriber.onError(IllegalArgumentException(s"a subscriber must request a positive amount, got $n")))
          else
            demand.getAndUpdate(current => if current + n < 0 then Long.MaxValue else current + n): Unit
            drain()

        def cancel(): Unit = done.set(true)

        private def finish(signal: => Unit): Unit =
          if done.compareAndSet(false, true)
          then signal

        private def drain(): Unit =
          if draining.getAndIncrement() == 0
          then
            var missed = 1

            while missed != 0 do
              try
                val iterator = remaining.getOrElse {
                  val started = values().iterator
                  remaining = Some(started)
                  started
                }

                while !done.get && demand.get > 0 && iterator.hasNext do
                  demand.decrementAndGet(): Unit
                  subscriber.onNext(iterator.next())

                if !iterator.hasNext
                then finish(subscriber.onComplete())
              catch case NonFatal(error) => finish(subscriber.onError(error))

              missed = draining.addAndGet(-missed)
            end while
        end drain
    )
end DeferredPublisher
