package mongo4s.kyo

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.duration.FiniteDuration

import kyo.*

import mongo4s.{Effect, ExitCase}

trait KyoEffectInstance:

  given kyoEffect: Effect[KIO] = new Effect[KIO]:
    def pure[A](a: A): KIO[A]                                          = a
    def delay[A](a: => A): KIO[A]                                      = Sync.defer(Abort.catching(a))
    def map[A, B](fa: KIO[A])(f: A => B): KIO[B]                       = fa.map(a => Abort.catching(f(a)))
    def flatMap[A, B](fa: KIO[A])(f: A => KIO[B]): KIO[B]              = fa.map(a => Abort.catching(f(a)))
    def raiseError[A](error: Throwable): KIO[A]                        = Abort.fail(error)
    def handleErrorWith[A](fa: KIO[A])(f: Throwable => KIO[A]): KIO[A] = Abort.recover[Throwable](f, f)(fa)

    override def suspend[A](fa: => KIO[A]): KIO[A] = Sync.defer(fa)

    override def sleep(duration: FiniteDuration): KIO[Unit] = Async.sleep(Duration.fromScala(duration))

    def guaranteeCase[A](fa: KIO[A])(finalizer: ExitCase => KIO[Unit]): KIO[A] =
      Sync.defer {
        val ended            = AtomicReference[ExitCase](ExitCase.Canceled)
        val finalizerFailure = AtomicReference[Maybe[Throwable]](Absent)

        val guarded: KIO[A] = Scope.run(
          Scope.ensure {
            handleErrorWith(runFinalizer(ended.get, finalizer)) { error =>
              delay(finalizerFailure.set(Present(error)))
            }
          }.map { _ =>
            Abort.run[Throwable](fa).map { result =>
              ended.set(KyoEffectInstance.exitCaseOf(result))
              Abort.get(result)
            }
          }
        )

        guarded.map { value =>
          finalizerFailure.get match
            case Present(error) => raiseError(error)
            case Absent         => pure(value)
        }
      }
    end guaranteeCase

    override def bracketCase[A, B](acquire: KIO[A])(use: A => KIO[B])(release: (A, ExitCase) => KIO[Unit]): KIO[B] =
      Sync.defer {
        val claimed = AtomicBoolean(false)
        val settled = CompletableFuture[Maybe[A]]()

        val owned: KIO[A] = suspend {
          if claimed.compareAndSet(false, true)
          then
            val acquired = map(acquire) { resource =>
              settled.complete(Present(resource)): Unit
              resource
            }

            handleErrorWith(acquired)(error => flatMap(delay(settled.complete(Absent): Unit))(_ => raiseError(error)))
          else raiseError(IllegalStateException("bracketCase was stopped before its acquisition began"))
        }

        guaranteeCase(flatMap(Async.uninterruptible(owned))(use)) { exitCase =>
          def releasing(acquired: Maybe[A]): KIO[Unit] =
            acquired match
              case Present(resource) => release(resource, exitCase)
              case Absent            => unit

          if claimed.compareAndSet(false, true) then unit
          else if settled.isDone then releasing(settled.getNow(Absent))
          else flatMap(Async.fromCompletableFuture(settled))(releasing)
        }
      }
    end bracketCase

object KyoEffectInstance extends KyoEffectInstance:

  private def exitCaseOf(result: Result[Throwable, Any]): ExitCase =
    result.foldError(
      _ => ExitCase.Succeeded,
      _.failureOrPanic match
        case _: Interrupted => ExitCase.Canceled
        case error          => ExitCase.Errored(error)
    )
