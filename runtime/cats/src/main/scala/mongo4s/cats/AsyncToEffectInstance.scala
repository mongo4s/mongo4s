package mongo4s.cats

import cats.effect.kernel.{Async, Outcome}

import mongo4s.{Effect, ExitCase}

trait AsyncToEffectInstance:

  given asyncEffect[F[*]](using F: Async[F]): Effect[F] = new Effect[F]:
    def pure[A](a: A): F[A]                                      = F.pure(a)
    def delay[A](a: => A): F[A]                                  = F.delay(a)
    def map[A, B](fa: F[A])(f: A => B): F[B]                     = F.map(fa)(f)
    def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]              = F.flatMap(fa)(f)
    def raiseError[A](error: Throwable): F[A]                    = F.raiseError(error)
    def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] = F.handleErrorWith(fa)(f)

    override def suspend[A](fa: => F[A]): F[A]                 = F.defer(fa)
    override def attempt[A](fa: F[A]): F[Either[Throwable, A]] = F.attempt(fa)

    def guaranteeCase[A](fa: F[A])(finalizer: ExitCase => F[Unit]): F[A] =
      F.guaranteeCase(fa)(outcome => runFinalizer(AsyncToEffectInstance.exitCaseOf(outcome), finalizer))

    override def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase) => F[Unit]): F[B] =
      F.bracketCase(acquire)(use) { (a, outcome) =>
        runFinalizer(
          AsyncToEffectInstance.exitCaseOf(outcome),
          exitCase => release(a, exitCase),
        )
      }

object AsyncToEffectInstance extends AsyncToEffectInstance:

  private def exitCaseOf[F[*], A](outcome: Outcome[F, Throwable, A]): ExitCase =
    outcome match
      case Outcome.Succeeded(_) => ExitCase.Succeeded
      case Outcome.Errored(err) => ExitCase.Errored(err)
      case Outcome.Canceled()   => ExitCase.Canceled
