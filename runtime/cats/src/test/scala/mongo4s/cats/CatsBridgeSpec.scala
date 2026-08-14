package mongo4s.cats

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import mongo4s.cats.CatsInstances.given
import mongo4s.{Effect, RsBridge}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

object CatsBridgeSpec:
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

final class CatsBridgeSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import CatsBridgeSpec.ListPublisher

  private val bridge = summon[RsBridge[IO, CatsStream[IO]]]
  private val effect = summon[Effect[IO]]

  "cats RsBridge" should {
    "collect a list" in {
      bridge.list(ListPublisher(List(1, 2, 3))).asserting(_ shouldBe List(1, 2, 3))
    }
    "take the first element" in {
      bridge.one(ListPublisher(List(10, 20))).asserting(_ shouldBe 10)
    }
    "return None for an empty option" in {
      bridge.option(ListPublisher(List.empty[Int])).asserting(_ shouldBe None)
    }
    "drain to unit" in {
      bridge.unit(ListPublisher(List(1, 2))).asserting(_ shouldBe ())
    }
    "expose a stream" in {
      bridge.stream(ListPublisher(List(1, 2, 3))).compile.toList.asserting(_ shouldBe List(1, 2, 3))
    }
  }

  "cats Effect" should {
    "sequence pure/map/flatMap" in {
      val program = effect.flatMap(effect.pure(2))(a => effect.map(effect.pure(3))(_ * a))
      program.asserting(_ shouldBe 6)
    }
    "raise and surface errors" in {
      effect.raiseError[Int](new RuntimeException("boom")).attempt.asserting(_.isLeft shouldBe true)
    }
  }
