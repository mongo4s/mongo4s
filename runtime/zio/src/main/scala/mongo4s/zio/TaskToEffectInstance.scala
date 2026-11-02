package mongo4s.zio

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import zio.{Cause, Clock, Duration, Exit, Task, ZIO}

import mongo4s.{Effect, ExitCase}

trait TaskToEffectInstance:
  given taskEffect: Effect[Task] = new Effect[Task]:
    def pure[A](a: A): Task[A]                               = ZIO.succeed(a)
    def delay[A](a: => A): Task[A]                           = ZIO.attempt(a)
    def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.flatMap(a => TaskToEffectInstance.failing(Exit.succeed(f(a))))
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(a => TaskToEffectInstance.failing(f(a)))
    def raiseError[A](error: Throwable): Task[A]             = ZIO.fail(error)

    override def suspend[A](fa: => Task[A]): Task[A] = ZIO.suspend(fa)

    override def monotonic: Task[FiniteDuration] = Clock.nanoTime.map(FiniteDuration(_, TimeUnit.NANOSECONDS))

    override def sleep(duration: FiniteDuration): Task[Unit] = ZIO.sleep(Duration.fromScala(duration))

    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] =
      fa.catchAllCause: cause =>
        if cause.isInterrupted
        then ZIO.refailCause(cause)
        else
          cause.failureOption match
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
          restore(TaskToEffectInstance.failing(use(a))).exit.flatMap { exit =>
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
  private inline def failing[A](inline task: Task[A]): Task[A] =
    try task
    catch case NonFatal(error) => Exit.fail(error)

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
