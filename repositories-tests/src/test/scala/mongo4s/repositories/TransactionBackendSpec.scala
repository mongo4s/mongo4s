package mongo4s.repositories

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.FiniteDuration

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.reactivestreams.{Publisher, Subscriber, Subscription}
import com.mongodb.MongoException
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.TransactionOptions
import mongo4s.{Effect, RsBridge, withTransaction}

import scala.concurrent.duration.given

object TransactionBackendSpec:

  def transient(message: String): MongoException =
    val error = MongoException(message)
    error.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
    error

  def unknownCommit(message: String): MongoException =
    val error = MongoException(message)
    error.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
    error

  private def publisher(error: Option[Throwable]): Publisher[Void] =
    (subscriber: Subscriber[? >: Void]) =>
      subscriber.onSubscribe(
        new Subscription:
          def request(n: Long): Unit = ()
          def cancel(): Unit         = ()
      )

      error match
        case Some(cause) => subscriber.onError(cause)
        case None        => subscriber.onComplete()

  /** A `ClientSession` that counts what was asked of it and fails the commit as many times as it is told to. */
  final class FakeSession(commitFailures: List[Throwable] = Nil):
    val started = AtomicInteger(0)
    val commits = AtomicInteger(0)
    val aborts  = AtomicInteger(0)

    val session: ClientSession =
      Proxy
        .newProxyInstance(
          classOf[ClientSession].getClassLoader,
          Array(classOf[ClientSession]),
          new InvocationHandler:
            def invoke(proxy: Object, method: Method, args: Array[Object]): Object =
              method.getName match
                case "startTransaction"  =>
                  started.incrementAndGet(): Unit
                  null
                case "commitTransaction" =>
                  val attempt = commits.getAndIncrement()
                  publisher(commitFailures.lift(attempt))
                case "abortTransaction"  =>
                  aborts.incrementAndGet(): Unit
                  publisher(None)
                case "close"             => null
                case "hashCode"          => Integer.valueOf(System.identityHashCode(proxy))
                case "equals"            => java.lang.Boolean.valueOf(proxy eq args(0))
                case "toString"          => "FakeSession"
                case other               => throw UnsupportedOperationException(s"FakeSession: $other is not simulated"),
        )
        .asInstanceOf[ClientSession]

trait TransactionBackendSpec[F[*], S[*]] extends AnyWordSpec, Matchers:
  import TransactionBackendSpec.*

  protected def effectInstance: Effect[F]

  protected def bridgeInstance: RsBridge[F, S]

  protected def run[A](fa: F[A]): A

  private given Effect[F]      = effectInstance
  private given RsBridge[F, S] = bridgeInstance
  private def F: Effect[F]     = effectInstance

  private def runAttempt[A](fa: F[A]): Either[Throwable, A] = run(F.attempt(fa))

  /** A body that raises the given errors on its first calls, then returns 1. */
  private def bodyFailing(errors: List[Throwable]): (AtomicInteger, F[Int]) =
    val calls = AtomicInteger(0)
    val body  = F.suspend {
      val attempt = calls.getAndIncrement()
      errors.lift(attempt).fold(F.pure(1))(F.raiseError)
    }
    (calls, body)

  "withTransaction retries" should {

    "restart the whole transaction after a transient error, then commit once" in {
      val fake          = FakeSession()
      val (calls, body) = bodyFailing(List(transient("first"), transient("second")))

      run(fake.session.withTransaction[F, S, Int](body)) shouldBe 1

      calls.get shouldBe 3
      fake.started.get shouldBe 3
      fake.aborts.get shouldBe 2
      fake.commits.get shouldBe 1
    }

    "retry only the commit when its result is unknown, without redoing the body" in {
      val fake          = FakeSession(commitFailures = List(unknownCommit("first")))
      val (calls, body) = bodyFailing(Nil)

      run(fake.session.withTransaction[F, S, Int](body)) shouldBe 1

      calls.get shouldBe 1
      fake.started.get shouldBe 1
      fake.commits.get shouldBe 2
      fake.aborts.get shouldBe 0
    }

    "leave an error the server did not label alone" in {
      val boom          = RuntimeException("not labelled")
      val fake          = FakeSession()
      val (calls, body) = bodyFailing(List(boom))

      runAttempt(fake.session.withTransaction[F, S, Int](body)) shouldBe Left(boom)

      calls.get shouldBe 1
      fake.started.get shouldBe 1
      fake.aborts.get shouldBe 1
    }

    "report the first failure when retries are switched off" in {
      val boom          = transient("still transient")
      val fake          = FakeSession()
      val (calls, body) = bodyFailing(List(boom))

      runAttempt(fake.session.withTransaction[F, S, Int](body, TransactionOptions.withoutRetries)) shouldBe Left(boom)

      calls.get shouldBe 1
      fake.started.get shouldBe 1
      fake.aborts.get shouldBe 1
    }

    "stop retrying once the timeout is spent" in {
      val boom          = transient("always transient")
      val fake          = FakeSession()
      val (calls, body) = bodyFailing(List.fill(50)(boom))

      val spent = TransactionOptions.default.withRetryTimeout(FiniteDuration(0, "nanoseconds"))

      runAttempt(fake.session.withTransaction[F, S, Int](body, spent)) shouldBe Left(boom)

      calls.get shouldBe 1
      fake.started.get shouldBe 1
    }

    "keep retrying while the timeout allows it" in {
      val fake          = FakeSession()
      val (calls, body) = bodyFailing(List.fill(5)(transient("transient")))

      val generous = TransactionOptions.default.withRetryTimeout(30.seconds)

      run(fake.session.withTransaction[F, S, Int](body, generous)) shouldBe 1

      calls.get shouldBe 6
      fake.started.get shouldBe 6
    }
  }

  "Effect.monotonic" should {
    "not go backwards" in {
      val first  = run(F.monotonic)
      val second = run(F.monotonic)

      (second >= first) shouldBe true
    }
  }
