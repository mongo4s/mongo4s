package mongo4s.kyo

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import kyo.{AllowUnsafe, Duration, KyoApp, Sync}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{Effect, RsBridge, RsBridgeConfig, RsBridgeError}

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
    "stream all elements" in {
      run(bridge.stream(ListPublisher(List(1, 2, 3))).run).toList shouldBe List(1, 2, 3)
    }
  }

  "kyo RsBridge with strictSingleResult" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(strictSingleResult = true)
    val strictBridge     = summon[RsBridge[KIO, KStream]]

    "fail one with TooManyResults for a 2-element publisher" in {
      intercept[RsBridgeError.TooManyResults](run(strictBridge.one(ListPublisher(List(1, 2))))) shouldBe RsBridgeError.TooManyResults(2)
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
    given RsBridgeConfig = RsBridgeConfig.Default.copy(timeout = Some(50.millis))
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
    "evaluate delay's thunk (eagerly - see the KNOWN LIMITATION note on KyoEffectInstance)" in {
      var evaluated = false
      val io        = effect.delay { evaluated = true; 42 }
      run(io) shouldBe 42
      evaluated shouldBe true
    }
  }
