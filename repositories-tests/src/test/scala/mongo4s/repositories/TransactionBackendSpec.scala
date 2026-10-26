package mongo4s.repositories

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration.{Duration, FiniteDuration}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonDocument
import com.mongodb.bulk.WriteConcernError
import com.mongodb.reactivestreams.client.ClientSession
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import com.mongodb.{MongoException, MongoExecutionTimeoutException, MongoWriteConcernException, ServerAddress}

import mongo4s.operations.TransactionOptions
import mongo4s.{Effect, MongoSession, RsBridge, withTransaction}

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

  def commitOutOfTime(error: MongoException): MongoException =
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

  final class FakeSession(commitFailures: List[Throwable] = Nil):
    val started = AtomicInteger(0)
    val commits = AtomicInteger(0)
    val aborts  = AtomicInteger(0)
    val active  = AtomicBoolean(false)

    val session: ClientSession =
      Proxy
        .newProxyInstance(
          classOf[ClientSession].getClassLoader,
          Array(classOf[ClientSession]),
          new InvocationHandler:
            def invoke(proxy: Object, method: Method, args: Array[Object]): Object =
              method.getName match
                case "startTransaction"     =>
                  started.incrementAndGet(): Unit
                  active.set(true)
                  null
                case "commitTransaction"    =>
                  val attempt = commits.getAndIncrement()
                  active.set(false)
                  publisher(commitFailures.lift(attempt))
                case "abortTransaction"     =>
                  aborts.incrementAndGet(): Unit
                  if active.getAndSet(false)
                  then publisher(None)
                  else publisher(Some(IllegalStateException("Cannot call abortTransaction after calling commitTransaction")))
                case "hasActiveTransaction" => java.lang.Boolean.valueOf(active.get)
                case "close"                => null
                case "hashCode"             => Integer.valueOf(System.identityHashCode(proxy))
                case "equals"               => java.lang.Boolean.valueOf(proxy eq args(0))
                case "toString"             => "FakeSession"
                case other                  => throw UnsupportedOperationException(s"FakeSession: $other is not simulated"),
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

    "not retry a commit that ran out of maxCommitTime, whether the server reports it at the top level or in its write concern" in {
      val topLevel     = commitOutOfTime(MongoExecutionTimeoutException(50, "MaxTimeMSExpired"))
      val writeConcern = commitOutOfTime(
        MongoWriteConcernException(WriteConcernError(50, "MaxTimeMSExpired", "wtimeout", BsonDocument()), null, ServerAddress(), java.util.Collections.emptyList())
      )

      List(topLevel, writeConcern).foreach { failure =>
        val fake          = FakeSession(commitFailures = List(failure))
        val (calls, body) = bodyFailing(Nil)

        runAttempt(fake.session.withTransaction[F, S, Int](body)).isLeft shouldBe true

        calls.get shouldBe 1
        fake.commits.get shouldBe 1
      }
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

    "not abort a transaction its failed commit already ended, so the error carries nothing it did not cause" in {
      val boom          = RuntimeException("commit failed")
      val fake          = FakeSession(commitFailures = List(boom))
      val (calls, body) = bodyFailing(Nil)

      runAttempt(fake.session.withTransaction[F, S, Int](body)) shouldBe Left(boom)

      calls.get shouldBe 1
      fake.aborts.get shouldBe 0
      boom.getSuppressed shouldBe empty
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

  "the pause before a transaction is retried" should {
    "stay below 5 ms grown by half per attempt, capped at 500 ms, and be jittered" in {
      List(1 -> 7500.micros, 2 -> 11250.micros, 12 -> 500.millis, 50 -> 500.millis).foreach { (attempt, ceiling) =>
        val pauses = List.fill(200)(run(MongoSession.backoff[F](attempt)))

        pauses.forall(pause => pause >= Duration.Zero && pause < ceiling) shouldBe true
        pauses.distinct.size should be > 1
      }
    }
  }

  "a transaction body" should {
    "be aborted when it throws while being built, before it has returned an F" in {
      val boom = IllegalArgumentException("thrown while building the body")
      val fake = FakeSession()

      runAttempt(fake.session.withTransaction[F, S, Int](throw boom)) shouldBe Left(boom)

      fake.started.get shouldBe 1
      fake.aborts.get shouldBe 1
    }
  }

  "Effect.monotonic" should {
    "not go backwards" in {
      val first  = run(F.monotonic)
      val second = run(F.monotonic)

      (second >= first) shouldBe true
    }
  }
