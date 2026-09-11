package mongo4s.benchmarks

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.openjdk.jmh.annotations.*
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.internal.RsBridgeSupport

object ErrorTranslationBenchmark:

  final class CountingPublisher(elements: Int) extends Publisher[Integer]:
    def subscribe(subscriber: Subscriber[? >: Integer]): Unit =
      subscriber.onSubscribe(
        new Subscription:
          private var emitted = 0
          private var done    = false

          def request(n: Long): Unit =
            if !done then
              while emitted < elements do
                subscriber.onNext(emitted)
                emitted += 1
              done = true
              subscriber.onComplete()

          def cancel(): Unit = done = true
      )

  def drain(publisher: Publisher[Integer]): Long =
    val total = AtomicLong(0L)

    publisher.subscribe(
      new Subscriber[Integer]:
        def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue)
        def onNext(value: Integer): Unit                  = total.addAndGet(value.longValue): Unit
        def onError(error: Throwable): Unit               = ()
        def onComplete(): Unit                            = ()
    )

    total.get()
  end drain

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class ErrorTranslationBenchmark:
  import ErrorTranslationBenchmark.*

  @Param(Array("1", "100", "10000"))
  var elements: Int = 0

  private var source: Publisher[Integer] = null

  @Setup(Level.Trial)
  def setup(): Unit = source = CountingPublisher(elements)

  @Benchmark def untranslated: Long = drain(source)

  @Benchmark def translated: Long = drain(RsBridgeSupport.translating(source))
