package mongo4s.zio

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import zio.{Runtime, Task, Unsafe}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{Effect, RsBridge, RsBridgeConfig, RsBridgeError}

import scala.concurrent.duration.given
import mongo4s.zio.ZioInstances.given

object ZioBridgeSpec:
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

  final class NeverPublisher[A]() extends Publisher[A]:
    def subscribe(subscriber: Subscriber[? >: A]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = ()
      )

final class ZioBridgeSpec extends AnyWordSpec, Matchers:
  import ZioBridgeSpec.{ListPublisher, NeverPublisher}

  private val bridge = summon[RsBridge[Task, ZioStream]]
  private val effect = summon[Effect[Task]]

  private def run[A](task: Task[A]): A =
    Unsafe.unsafe(implicit unsafe => Runtime.default.unsafe.run(task).getOrThrow())

  "zio RsBridge" should {
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
    "expose a stream" in {
      run(bridge.stream(ListPublisher(List(1, 2, 3))).runCollect).toList shouldBe List(1, 2, 3)
    }
  }

  "zio RsBridge with strictSingleResult" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(strictSingleResult = true)
    val strictBridge     = summon[RsBridge[Task, ZioStream]]

    "fail one with TooManyResults for a 2-element publisher" in {
      run(strictBridge.one(ListPublisher(List(1, 2))).either) shouldBe Left(RsBridgeError.TooManyResults(2))
    }
    "fail one with EmptyResult for an empty publisher" in {
      run(strictBridge.one(ListPublisher(List.empty[Int])).either) shouldBe Left(RsBridgeError.EmptyResult())
    }
  }

  "zio RsBridge with strictSingleResult disabled (default)" should {
    "still return the first element for a 2-element publisher (regression guard)" in {
      run(bridge.one(ListPublisher(List(1, 2)))) shouldBe 1
    }
  }

  "zio RsBridge with a timeout" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(timeout = Some(50.millis))
    val timeoutBridge    = summon[RsBridge[Task, ZioStream]]

    "fail with RsBridgeError.Timeout when the publisher never completes" in {
      run(timeoutBridge.list(NeverPublisher[Int]()).either) shouldBe Left(RsBridgeError.Timeout(50.millis))
    }
  }

  "zio Effect" should {
    "sequence pure/map/flatMap" in {
      run(effect.flatMap(effect.pure(2))(a => effect.map(effect.pure(3))(_ * a))) shouldBe 6
    }
    "raise and surface errors" in {
      run(effect.raiseError[Int](new RuntimeException("boom")).either).isLeft shouldBe true
    }
  }
