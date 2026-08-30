package mongo4s.cats

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{Effect, RsBridge, RsBridgeConfig, RsBridgeError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

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

  final class NeverPublisher[A] extends Publisher[A]:
    def subscribe(subscriber: Subscriber[? >: A]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = ()
      )

final class CatsBridgeSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import CatsBridgeSpec.{ListPublisher, NeverPublisher}

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

  "cats RsBridge with strictSingleResult" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(strictSingleResult = true)
    val strictBridge     = summon[RsBridge[IO, CatsStream[IO]]]

    "fail one with TooManyResults for a 2-element publisher" in {
      strictBridge.one(ListPublisher(List(1, 2))).attempt.asserting(_ shouldBe Left(RsBridgeError.TooManyResults()))
    }
    "fail one with EmptyResult for an empty publisher" in {
      strictBridge.one(ListPublisher(List.empty[Int])).attempt.asserting(_ shouldBe Left(RsBridgeError.EmptyResult()))
    }
  }

  "cats RsBridge with strictSingleResult disabled (default)" should {
    "still return the first element for a 2-element publisher (regression guard)" in {
      bridge.one(ListPublisher(List(1, 2))).asserting(_ shouldBe 1)
    }
  }

  "cats RsBridge with a timeout" should {
    given RsBridgeConfig = RsBridgeConfig.Default.copy(timeout = Some(50.millis))
    val timeoutBridge    = summon[RsBridge[IO, CatsStream[IO]]]

    "fail with RsBridgeError.Timeout when the publisher never completes" in {
      timeoutBridge.list(NeverPublisher[Int]()).attempt.asserting(_ shouldBe Left(RsBridgeError.Timeout(50.millis)))
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
