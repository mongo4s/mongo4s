package mongo4s.repositories

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import com.mongodb.{MongoException, ServerAddress, WriteError, MongoWriteException}

import mongo4s.{MongoError, RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

import scala.concurrent.duration.given

object RsBridgeBackendSpec:

  final class RecordingPublisher(items: List[Int], completeWhenExhausted: Boolean = true) extends Publisher[Int]:
    val delivered = AtomicInteger(0)
    val cancelled = AtomicBoolean(false)

    def subscribe(subscriber: Subscriber[? >: Int]): Unit =
      val index     = AtomicInteger(0)
      val completed = AtomicBoolean(false)

      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit =
            var remaining = n
            while remaining > 0 && index.get < items.size && !cancelled.get do
              val next = index.getAndIncrement()
              if next < items.size then
                delivered.incrementAndGet()
                subscriber.onNext(items(next))
              remaining -= 1

            if completeWhenExhausted && index.get >= items.size && !cancelled.get
              && completed.compareAndSet(false, true)
            then subscriber.onComplete()

          def cancel(): Unit = cancelled.set(true)
      )

  final class EndlessPublisher extends Publisher[Int]:
    val cancelled = AtomicBoolean(false)

    def subscribe(subscriber: Subscriber[? >: Int]): Unit =
      val demand = AtomicLong(0)
      val next   = AtomicInteger(0)

      val emitter = Thread(
        () =>
          while !cancelled.get do
            if demand.get > 0 then
              demand.decrementAndGet()
              subscriber.onNext(next.getAndIncrement())
            else Thread.sleep(1)
        ,
        "endless-publisher",
      )
      emitter.setDaemon(true)

      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = demand.addAndGet(n): Unit
          def cancel(): Unit         = cancelled.set(true)
      )

      emitter.start()

  final class FailingPublisher(error: Throwable) extends Publisher[Int]:
    def subscribe(subscriber: Subscriber[? >: Int]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = subscriber.onError(error)
          def cancel(): Unit         = ()
      )

  final class NeverPublisher extends Publisher[Int]:
    val cancelled = AtomicBoolean(false)

    def subscribe(subscriber: Subscriber[? >: Int]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = cancelled.set(true)
      )

trait RsBridgeBackendSpec[F[*], S[*]] extends AnyWordSpec, Matchers, TimeLimits:
  import RsBridgeBackendSpec.*

  protected def bridgeWith(config: RsBridgeConfig): RsBridge[F, S]

  protected def streamableInt: Streamable[S, Int]

  private given Streamable[S, Int] = streamableInt

  protected def run[A](fa: F[A]): A

  protected def takeFromStream(stream: S[Int], n: Int): List[Int]

  protected def drainStream(stream: S[Int]): List[Int]

  private val bridge = bridgeWith(RsBridgeConfig.default)

  private def attempt[A](fa: F[A]): Either[Throwable, A] =
    try Right(run(fa))
    catch case error: Throwable => Left(error)

  private def unwrapped(error: Throwable): Throwable =
    LazyList
      .iterate(Option(error))(_.flatMap(e => Option(e.getCause)))
      .takeWhile(_.isDefined)
      .flatten
      .collectFirst { case typed: MongoError => typed }
      .getOrElse(error)

  private given Signaler = ThreadSignaler

  private def promptly[A](body: => A): A = failAfter(Span(30, Seconds))(body)

  "list" should {
    "collect every element" in {
      run(bridge.list(RecordingPublisher(List(1, 2, 3)))) shouldBe List(1, 2, 3)
    }
  }

  "one" should {
    "return the first element" in {
      run(bridge.one(RecordingPublisher(List(10, 20)))) shouldBe 10
    }

    "stop reading instead of draining the cursor" in {
      val publisher = RecordingPublisher((1 to 10000).toList)

      run(bridge.one(publisher)) shouldBe 1
      publisher.delivered.get should be < 1000
    }
  }

  "option" should {
    "return None for an empty publisher" in {
      run(bridge.option(RecordingPublisher(List.empty))) shouldBe None
    }

    "return the first element for a non-empty publisher" in {
      run(bridge.option(RecordingPublisher(List(7, 8)))) shouldBe Some(7)
    }
  }

  "unit" should {
    "consume the publisher and produce unit" in {
      run(bridge.unit(RecordingPublisher(List(1, 2)))) shouldBe ()
    }

    "report the publisher's own error rather than a wrapper around it" in {
      val boom = RuntimeException("drained")

      attempt(bridge.unit(FailingPublisher(boom))) shouldBe Left(boom)
    }
  }

  "stream" should {
    "emit every element of a finite publisher" in {
      drainStream(bridge.stream(RecordingPublisher(List(1, 2, 3)))) shouldBe List(1, 2, 3)
    }

    "deliver elements from a publisher that never completes" in {
      val publisher = EndlessPublisher()

      promptly {
        takeFromStream(bridge.stream(publisher), 3) shouldBe List(0, 1, 2)
      }
    }
  }

  "errors" should {
    "propagate an onError signal from the publisher" in {
      val boom = RuntimeException("publisher failed")

      attempt(bridge.list(FailingPublisher(boom))).left.map(_.getMessage) shouldBe Left("publisher failed")
    }
  }

  "a driver failure" should {

    def duplicateKey: MongoWriteException =
      MongoWriteException(WriteError(11000, "E11000 duplicate key", org.bson.BsonDocument()), ServerAddress())

    def translated[A](fa: F[A]): Throwable =
      attempt(fa).left.getOrElse(fail("expected the operation to fail"))

    "arrive typed from one" in {
      translated(bridge.one(FailingPublisher(duplicateKey))) shouldBe a[MongoError.DuplicateKey]
    }

    "arrive typed from option" in {
      translated(bridge.option(FailingPublisher(duplicateKey))) shouldBe a[MongoError.DuplicateKey]
    }

    "arrive typed from list" in {
      translated(bridge.list(FailingPublisher(duplicateKey))) shouldBe a[MongoError.DuplicateKey]
    }

    "arrive typed from unit" in {
      translated(bridge.unit(FailingPublisher(duplicateKey))) shouldBe a[MongoError.DuplicateKey]
    }

    "arrive typed from stream, which does not share the other four's plumbing" in {
      val failed =
        try
          drainStream(bridge.stream(FailingPublisher(duplicateKey)))
          fail("expected the stream to fail")
        catch case error: Throwable => error

      unwrapped(failed) shouldBe a[MongoError.DuplicateKey]
    }

    "keep the driver's exception reachable as the cause" in {
      val original = duplicateKey

      translated(bridge.one(FailingPublisher(original))).getCause shouldBe original
    }

    "leave an error the driver did not raise alone" in {
      val boom = RuntimeException("not a driver failure")

      translated(bridge.one(FailingPublisher(boom))) shouldBe boom
    }
  }

  "strictSingleResult" should {
    val strict = bridgeWith(RsBridgeConfig.default.withStrictSingleResult)

    "fail with TooManyResults when more than one element is available" in {
      attempt(strict.one(RecordingPublisher(List(1, 2)))) shouldBe Left(RsBridgeError.TooManyResults())
    }

    "fail with EmptyResult when nothing is available" in {
      attempt(strict.one(RecordingPublisher(List.empty))) shouldBe Left(RsBridgeError.EmptyResult())
    }

    "still return the first element when disabled (regression guard)" in {
      run(bridge.one(RecordingPublisher(List(1, 2)))) shouldBe 1
    }
  }

  "timeout" should {
    val timed = bridgeWith(RsBridgeConfig.default.withTimeout(200.millis))

    "fail with RsBridgeError.Timeout when the publisher never responds" in {
      attempt(timed.list(NeverPublisher())) shouldBe Left(RsBridgeError.Timeout(200.millis))
    }

    "release the subscription when it times out" in {
      val publisher = NeverPublisher()

      attempt(timed.list(publisher)).isLeft shouldBe true
      eventuallyCancelled(publisher.cancelled) shouldBe true
    }
  }

  private def eventuallyCancelled(flag: AtomicBoolean): Boolean =
    val deadline = System.nanoTime() + 5.seconds.toNanos
    while !flag.get && System.nanoTime() < deadline do Thread.sleep(25)
    flag.get
