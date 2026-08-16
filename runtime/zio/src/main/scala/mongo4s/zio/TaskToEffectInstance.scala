package mongo4s.zio

import zio.{Task, ZIO}
import mongo4s.Effect

trait TaskToEffectInstance:

  given taskEffect: Effect[Task] with
    def pure[A](a: A): Task[A]                                                     = ZIO.succeed(a)
    def delay[A](a: => A): Task[A]                                                 = ZIO.attempt(a)
    def map[A, B](fa: Task[A])(f: A => B): Task[B]                                 = fa.map(f)
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B]                       = fa.flatMap(f)
    def raiseError[A](error: Throwable): Task[A]                                   = ZIO.fail(error)
    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A]          = fa.catchAll(f)

object TaskToEffectInstance extends TaskToEffectInstance
