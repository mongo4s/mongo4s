package mongo4s.rapid

import scala.util.{Failure, Success}

import rapid.Task

import mongo4s.{Effect, ExitCase}

trait TaskToEffectInstance:

  given rapidEffect: Effect[Task] = new Effect[Task]:
    def pure[A](a: A): Task[A]                                            = Task.pure(a)
    def delay[A](a: => A): Task[A]                                        = Task(a)
    def map[A, B](fa: Task[A])(f: A => B): Task[B]                        = fa.map(f)
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B]              = fa.flatMap(f)
    def raiseError[A](error: Throwable): Task[A]                          = Task.error(error)
    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.handleError(f)

    override def suspend[A](fa: => Task[A]): Task[A] = Task.pure(()).flatMap(_ => fa)

    def guaranteeCase[A](fa: Task[A])(finalizer: ExitCase => Task[Unit]): Task[A] =
      fa.attempt.flatMap { outcome =>
        val exitCase = outcome match
          case Success(_)     => ExitCase.Succeeded
          case Failure(error) => ExitCase.Errored(error)

        runFinalizer(exitCase, finalizer).map(_ => outcome.get)
      }

object TaskToEffectInstance extends TaskToEffectInstance
