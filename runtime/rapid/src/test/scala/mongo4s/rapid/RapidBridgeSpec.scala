package mongo4s.rapid

import rapid.Task
import mongo4s.rapid.RapidInstances.given
import mongo4s.{Effect, RsBridge}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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

final class RapidBridgeSpec extends AnyWordSpec, Matchers:
  import RapidBridgeSpec.ListPublisher

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

  "rapid Effect" should {
    "sequence pure/map/flatMap" in {
      effect.flatMap(effect.pure(2))(a => effect.map(effect.pure(3))(_ * a)).sync() shouldBe 6
    }
    "raise and surface errors" in {
      assertThrows[RuntimeException](effect.raiseError[Int](new RuntimeException("boom")).sync())
    }
  }
