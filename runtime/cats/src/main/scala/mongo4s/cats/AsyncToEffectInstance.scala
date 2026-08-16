package mongo4s.cats

import cats.effect.kernel.Async

import mongo4s.Effect

trait AsyncToEffectInstance:

  given asyncEffect[F[*]](using F: Async[F]): Effect[F] with
    def pure[A](a: A): F[A]                                       = F.pure(a)
    def delay[A](a: => A): F[A]                                   = F.delay(a)
    def map[A, B](fa: F[A])(f: A => B): F[B]                      = F.map(fa)(f)
    def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]               = F.flatMap(fa)(f)
    def raiseError[A](error: Throwable): F[A]                     = F.raiseError(error)
    def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A]  = F.handleErrorWith(fa)(f)

object AsyncToEffectInstance extends AsyncToEffectInstance
