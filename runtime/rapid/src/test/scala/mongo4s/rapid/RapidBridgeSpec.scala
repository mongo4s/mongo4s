package mongo4s.rapid

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import rapid.Task
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{Effect, RsBridge, RsBridgeConfig, RsBridgeError}

import scala.concurrent.duration.given
import mongo4s.rapid.RapidInstances.given

object RapidBridgeSpec:
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

final class RapidBridgeSpec extends AnyWordSpec, Matchers:
  import RapidBridgeSpec.{ListPublisher, NeverPublisher}

  private val bridge = summon[RsBridge[Task, RapidStream]]
  private val effect = summon[Effect[Task]]

  "rapid RsBridge" should {
    "collect a list" in {
      bridge.list(ListPublisher(List(1, 2, 3))).sync() shouldBe List(1, 2, 3)
    }
    "take the first element" in {
      bridge.one(ListPublisher(List(10, 20))).sync() shouldBe 10
    }
    "return None for an empty option" in {
      bridge.option(ListPublisher(List.empty[Int])).sync() shouldBe None
    }
    "drain to unit" in {
      bridge.unit(ListPublisher(List(1, 2))).sync() shouldBe ()
    }
    "expose a stream" in {
      bridge.stream(ListPublisher(List(1, 2, 3))).toList.sync() shouldBe List(1, 2, 3)
    }
  }

  "rapid RsBridge with strictSingleResult" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(strictSingleResult = true)
    val strictBridge     = summon[RsBridge[Task, RapidStream]]

    "fail one with TooManyResults for a 2-element publisher" in {
      intercept[RsBridgeError.TooManyResults](strictBridge.one(ListPublisher(List(1, 2))).sync()) shouldBe RsBridgeError.TooManyResults()
    }
    "fail one with EmptyResult for an empty publisher" in {
      intercept[RsBridgeError.EmptyResult](strictBridge.one(ListPublisher(List.empty[Int])).sync()) shouldBe RsBridgeError.EmptyResult()
    }
  }

  "rapid RsBridge with strictSingleResult disabled (default)" should {
    "still return the first element for a 2-element publisher (regression guard)" in {
      bridge.one(ListPublisher(List(1, 2))).sync() shouldBe 1
    }
  }

  "rapid RsBridge with a timeout" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(timeout = Some(50.millis))
    val timeoutBridge    = summon[RsBridge[Task, RapidStream]]

    "fail with RsBridgeError.Timeout when the publisher never completes" in {
      intercept[RsBridgeError.Timeout](timeoutBridge.list(NeverPublisher[Int]()).sync()) shouldBe RsBridgeError.Timeout(50.millis)
    }
  }

  "rapid Effect" should {
    "sequence pure/map/flatMap" in {
      effect.flatMap(effect.pure(2))(a => effect.map(effect.pure(3))(_ * a)).sync() shouldBe 6
    }
    "raise and surface errors" in {
      assertThrows[RuntimeException](effect.raiseError[Int](new RuntimeException("boom")).sync())
    }
  }
