package mongo4s.kyo

import scala.compiletime.asMatchable

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

    def guaranteeCase[A](fa: KIO[A])(finalizer: ExitCase => KIO[Unit]): KIO[A] =
      val abnormal: KIO[A] = Scope.run(
        Scope.ensure {
          case Absent          => pure(())
          case Present(failed) => runFinalizer(KyoEffectInstance.exitCaseOf(Present(failed)), finalizer)
        }.map(_ => fa)
      )

      abnormal.map(value => runFinalizer(ExitCase.Succeeded, finalizer).map(_ => value))
    end guaranteeCase

object KyoEffectInstance extends KyoEffectInstance:

  private def exitCaseOf(error: Maybe[Result.Error[Any]]): ExitCase =
    error match
      case Absent          => ExitCase.Succeeded
      case Present(failed) =>
        failed match
          case Result.Panic(_: Interrupted) => ExitCase.Canceled
          case Result.Panic(exception)      => ExitCase.Errored(exception)
          case Result.Failure(failure)      =>
            failure.asMatchable match
              case error: Throwable => ExitCase.Errored(error)
              case other            => ExitCase.Errored(RuntimeException(String.valueOf(other)))
          case other                        => ExitCase.Errored(RuntimeException(String.valueOf(other)))
