package mongo4s.syntax

import scala.concurrent.duration.FiniteDuration

import com.mongodb.MongoException
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.TransactionOptions
import mongo4s.{Effect, ExitCase, MongoSession, RsBridge}

trait session:

  extension (session: ClientSession)
    def withTransaction[F[*], S[*], A](
        fa: Option[ClientSession] ?=> F[A],
        options: TransactionOptions = TransactionOptions.default,
    )(using F: Effect[F], rs: RsBridge[F, S]): F[A] =
      given Option[ClientSession] = Some(session)

      def deadline: F[Option[FiniteDuration]] =
        options.retryTimeout match
          case Some(timeout) => F.map(F.monotonic)(now => Some(now + timeout))
          case None          => F.pure(None)

      def retryable(until: Option[FiniteDuration]): F[Boolean] =
        until match
          case Some(limit) => F.map(F.monotonic)(_ < limit)
          case None        => F.pure(false)

      def orRaise[B](error: Throwable, until: Option[FiniteDuration])(retry: => F[B]): F[B] =
        F.flatMap(retryable(until)) { again =>
          if again
          then retry
          else F.raiseError(error)
        }

      def commit(until: Option[FiniteDuration]): F[Unit] =
        F.handleErrorWith(MongoSession.commitTransaction[F, S](session)) { error =>
          if MongoSession.hasErrorLabel(error, MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
          then orRaise(error, until)(commit(until))
          else F.raiseError(error)
        }

      def attempt(until: Option[FiniteDuration]): F[A] =
        F.flatMap(MongoSession.startTransaction[F](session, options)) { _ =>
          val bodyThenCommit = F.flatMap(fa)(a => F.map(commit(until))(_ => a))

          F.guaranteeCase(bodyThenCommit) {
            case ExitCase.Succeeded      => F.unit
            case ExitCase.Errored(error) => MongoSession.abortQuietly[F, S](session, Some(error))
            case ExitCase.Canceled       => MongoSession.abortQuietly[F, S](session, None)
          }
        }

      def run(until: Option[FiniteDuration]): F[A] =
        F.handleErrorWith(F.suspend(attempt(until))) { error =>
          if MongoSession.hasErrorLabel(error, MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
          then orRaise(error, until)(run(until))
          else F.raiseError(error)
        }

      F.flatMap(deadline)(run)
    end withTransaction

end session

object session extends session
