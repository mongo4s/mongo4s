package mongo4s.kyo

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import kyo.{Abort, AllowUnsafe, Async, Duration, Fiber, Kyo, KyoApp, Result, Sync}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{Effect, ExitCase, RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

object KyoBridgeSpec:
  final class ListPublisher[A](items: List[A]) extends Publisher[A]:
    def subscribe(subscriber: Subscriber[? >: A]): Unit =
      var index     = 0
      var cancelled = false
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit =
            var remaining = n
            while remaining > 0 && index < items.size && !cancelled do
              subscriber.onNext(items(index))
              index += 1
              remaining -= 1
            if index >= items.size && !cancelled then subscriber.onComplete()
          def cancel(): Unit         = cancelled = true
      )

  final class NeverPublisher[A] extends Publisher[A]:
    def subscribe(subscriber: Subscriber[? >: A]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = ()
      )

final class KyoBridgeSpec extends AnyWordSpec, Matchers:
  import KyoBridgeSpec.{ListPublisher, NeverPublisher}

  private val bridge = summon[RsBridge[KIO, KStream]]
  private val effect = summon[Effect[KIO]]

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  private def run[A](io: KIO[A]): A =
    Sync.Unsafe.evalOrThrow(KyoApp.runAndBlock(Duration.fromScala(5.seconds))(io))

  "kyo RsBridge" should {
    "collect a list" in {
      run(bridge.list(ListPublisher(List(1, 2, 3)))) shouldBe List(1, 2, 3)
    }
    "take the first element" in {
      run(bridge.one(ListPublisher(List(10, 20)))) shouldBe 10
    }
    "return None for an empty option" in {
      run(bridge.option(ListPublisher(List.empty[Int]))) shouldBe None
    }
    "drain to unit" in {
      run(bridge.unit(ListPublisher(List(1, 2)))) shouldBe ()
    }
    "explain itself when handed a Streamable it did not derive, rather than failing as a bad cast" in {
      val error = intercept[UnsupportedOperationException] {
        bridge.stream(ListPublisher(List(1, 2, 3)))(using Streamable.instance[KStream, Int])
      }

      error.getMessage should include("Tag")
    }

    "stream all elements" in {
      run(bridge.stream(ListPublisher(List(1, 2, 3))).run).toList shouldBe List(1, 2, 3)
    }

    "build the publisher of a stream when the stream runs, not when it is defined" in {
      var built  = 0
      val stream = bridge.stream { built += 1; ListPublisher(List(1, 2)) }

      built shouldBe 0
      run(stream.run).toList shouldBe List(1, 2)
      run(stream.run).toList shouldBe List(1, 2)
      built shouldBe 2
    }

    "fail the stream itself when its publisher cannot be built" in {
      val stream = bridge.stream[Int](throw IllegalArgumentException("no publisher"))

      run(Abort.run[Throwable](stream.run)) match
        case Result.Failure(error) => error.getMessage shouldBe "no publisher"
        case other                 => fail(s"expected a failure in the stream, got $other")
    }
  }

  "kyo RsBridge with strictSingleResult" should {
    given RsBridgeConfig = RsBridgeConfig.default.withStrictSingleResult
    val strictBridge     = summon[RsBridge[KIO, KStream]]

    "fail one with TooManyResults for a 2-element publisher" in {
      intercept[RsBridgeError.TooManyResults](run(strictBridge.one(ListPublisher(List(1, 2))))) shouldBe RsBridgeError.TooManyResults()
    }
    "fail one with EmptyResult for an empty publisher" in {
      intercept[RsBridgeError.EmptyResult](run(strictBridge.one(ListPublisher(List.empty[Int])))) shouldBe RsBridgeError.EmptyResult()
    }
  }

  "kyo RsBridge with strictSingleResult disabled (default)" should {
    "still return the first element for a 2-element publisher (regression guard)" in {
      run(bridge.one(ListPublisher(List(1, 2)))) shouldBe 1
    }
  }

  "kyo RsBridge with a timeout" should {
    given RsBridgeConfig = RsBridgeConfig.default.withTimeout(50.millis)
    val timeoutBridge    = summon[RsBridge[KIO, KStream]]

    "fail with RsBridgeError.Timeout when the publisher never completes" in {
      intercept[RsBridgeError.Timeout](run(timeoutBridge.list(NeverPublisher[Int]()))) shouldBe RsBridgeError.Timeout(50.millis)
    }
  }

  "kyo Effect" should {
    "sequence pure/map/flatMap" in {
      val program = effect.flatMap(effect.pure(2))(a => effect.map(effect.pure(3))(_ * a))
      run(program) shouldBe 6
    }
    "defer delay's thunk until the program runs" in {
      var evaluated = false
      val io        = effect.delay { evaluated = true; 42 }
      evaluated shouldBe false
      run(io) shouldBe 42
      evaluated shouldBe true
    }

    "release what was acquired when the fiber is interrupted while acquiring it" in {
      val acquiring = CountDownLatch(1)
      val proceed   = CountDownLatch(1)
      val released  = CountDownLatch(1)

      val program: KIO[Int] =
        effect.bracket(effect.delay { acquiring.countDown(); proceed.await(); "session" })(_ => Async.never[Int])(_ => effect.delay(released.countDown()))

      run(
        for
          fiber <- Fiber.initUnscoped(program)
          _     <- Sync.defer(acquiring.await())
          _     <- fiber.interrupt
          _     <- Sync.defer(proceed.countDown())
        yield ()
      )

      released.await(5, TimeUnit.SECONDS) shouldBe true
    }

    "release exactly what it acquired, wherever an interrupt lands" in {
      val acquired = AtomicInteger(0)
      val released = AtomicInteger(0)

      val program: KIO[Int] =
        effect.bracket(effect.delay(acquired.incrementAndGet()))(_ => Async.never[Int])(_ => effect.delay(released.incrementAndGet(): Unit))

      run(
        Kyo.foreachDiscard(1 to 200) { round =>
          for
            fiber <- Fiber.initUnscoped(program)
            _     <- if round % 2 == 0 then Async.sleep(Duration.fromScala(1.millis)) else Sync.defer(())
            _     <- fiber.interrupt
          yield ()
        }
      )

      val deadline = System.nanoTime() + 5.seconds.toNanos
      while released.get < acquired.get && System.nanoTime() < deadline do Thread.sleep(10)

      acquired.get should be > 0
      released.get shouldBe acquired.get
    }

    "run the finalizer of a body that succeeded, and raise the finalizer's failure" in {
      val ran  = AtomicInteger(0)
      val boom = IllegalStateException("finalizer")

      run(effect.guaranteeCase(effect.pure(1))(exitCase => effect.delay { ran.incrementAndGet(); exitCase shouldBe ExitCase.Succeeded; () })) shouldBe 1
      run(effect.attempt(effect.guaranteeCase(effect.pure(1))(_ => effect.raiseError(boom)))) shouldBe Left(boom)
      ran.get shouldBe 1
    }
  }
