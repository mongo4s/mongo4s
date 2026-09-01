package mongo4s.zio

import zio.{Cause, Exit, Task, ZIO}

import mongo4s.{Effect, ExitCase}

trait TaskToEffectInstance:

  given taskEffect: Effect[Task] = new Effect[Task]:
    def pure[A](a: A): Task[A]                               = ZIO.succeed(a)
    def delay[A](a: => A): Task[A]                           = ZIO.attempt(a)
    def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
    def raiseError[A](error: Throwable): Task[A]             = ZIO.fail(error)

    override def suspend[A](fa: => Task[A]): Task[A] = ZIO.suspendSucceed(fa)

    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] =
      fa.catchAllCause: cause =>
        TaskToEffectInstance.errorOf(cause) match
          case Some(error) => f(error)
          case None        => ZIO.refailCause(cause)

    def guaranteeCase[A](fa: Task[A])(finalizer: ExitCase => Task[Unit]): Task[A] =
      ZIO.uninterruptibleMask { restore =>
        restore(fa).exit.flatMap { exit =>
          runFinalizer(TaskToEffectInstance.exitCaseOf(exit), finalizer).foldCauseZIO(
            finalizerCause =>
              if exit.isSuccess
              then ZIO.refailCause(finalizerCause)
              else ZIO.suspendSucceed(exit),
            _ => ZIO.suspendSucceed(exit),
          )
        }
      }

    override def bracketCase[A, B](acquire: Task[A])(use: A => Task[B])(release: (A, ExitCase) => Task[Unit]): Task[B] =
      ZIO.uninterruptibleMask { restore =>
        acquire.flatMap { a =>
          restore(use(a)).exit.flatMap { exit =>
            runFinalizer(
              TaskToEffectInstance.exitCaseOf(exit),
              exitCase => release(a, exitCase),
            ).foldCauseZIO(
              finalizerCause =>
                if exit.isSuccess
                then ZIO.refailCause(finalizerCause)
                else ZIO.suspendSucceed(exit),
              _ => ZIO.suspendSucceed(exit),
            )
          }
        }
      }

object TaskToEffectInstance extends TaskToEffectInstance:

  private def errorOf(cause: Cause[Throwable]): Option[Throwable] =
    if cause.isInterrupted
    then None
    else cause.failureOption.orElse(cause.defects.headOption)

  private def exitCaseOf[A](exit: Exit[Throwable, A]): ExitCase = exit match
    case Exit.Success(_)     => ExitCase.Succeeded
    case Exit.Failure(cause) =>
      errorOf(cause) match
        case Some(error) => ExitCase.Errored(error)
        case None        => ExitCase.Canceled
