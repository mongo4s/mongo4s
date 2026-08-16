package mongo4s.rapid

import rapid.Task
import mongo4s.Effect

trait TaskToEffectInstance:

  given rapidEffect: Effect[Task] = new Effect[Task]:
    def pure[A](a: A): Task[A]                               = Task.pure(a)
    def delay[A](a: => A): Task[A]                           = Task(a)
    def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
    def raiseError[A](error: Throwable): Task[A]             = Task.error(error)
    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] = fa.handleError(f)

object TaskToEffectInstance extends TaskToEffectInstance
