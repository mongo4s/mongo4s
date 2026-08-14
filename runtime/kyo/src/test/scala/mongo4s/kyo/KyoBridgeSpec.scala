package mongo4s.kyo

import kyo.{AllowUnsafe, Duration, KyoApp, Sync}
import mongo4s.kyo.KyoInstances.given
import mongo4s.{Effect, RsBridge}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

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

final class KyoBridgeSpec extends AnyWordSpec, Matchers:
  import KyoBridgeSpec.ListPublisher

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
