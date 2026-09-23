package mongo4s.repositories

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.internal.PublisherIterator

final class PublisherIteratorSpec extends AnyWordSpec, Matchers:

  private def silent(cancelled: AtomicBoolean): Publisher[Int] =
    (subscriber: Subscriber[? >: Int]) =>
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = cancelled.set(true)
      )

  "cancelling an iterator parked on a silent publisher" should {

    "release the thread waiting in hasNext, rather than leaving it blocked forever" in {
      val cancelled = AtomicBoolean(false)
      val iterator  = PublisherIterator(silent(cancelled), 1)
      val entered   = CountDownLatch(1)
      val finished  = CountDownLatch(1)

      val consumer = Thread(() =>
        entered.countDown()
        if !iterator.hasNext then finished.countDown()
      )
      consumer.setDaemon(true)
      consumer.start()

      entered.await(5, TimeUnit.SECONDS) shouldBe true
      Thread.sleep(100)

      iterator.cancel()

      withClue("hasNext never returned after cancel — the consumer thread is leaked: ") {
        finished.await(5, TimeUnit.SECONDS) shouldBe true
      }

      cancelled.get shouldBe true
    }
  }

  "an iterator with a limit" should {

    "ask for no more than its limit, end there without waiting for more, and release the subscription" in {
      val requested = AtomicLong(0)
      val cancelled = AtomicBoolean(false)

      val endless: Publisher[Int] = (subscriber: Subscriber[? >: Int]) =>
        subscriber.onSubscribe(
          new Subscription:
            def request(n: Long): Unit = (1L to n).foreach(_ => subscriber.onNext(requested.incrementAndGet().toInt))
            def cancel(): Unit         = cancelled.set(true)
        )

      PublisherIterator(endless, 256, limit = 2).toList shouldBe List(1, 2)
      requested.get shouldBe 2
      cancelled.get shouldBe true
    }
  }
