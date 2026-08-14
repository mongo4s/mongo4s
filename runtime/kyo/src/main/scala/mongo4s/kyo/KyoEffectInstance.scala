package mongo4s.kyo

import kyo.Abort

import mongo4s.Effect

trait KyoEffectInstance:

  given kyoEffect: Effect[KIO] = new Effect[KIO]:
    def pure[A](a: A): KIO[A]     = a
    def delay[A](a: => A): KIO[A] = Abort.catching(a)

    def map[A, B](fa: KIO[A])(f: A => B): KIO[B]          = fa.map(f)
    def flatMap[A, B](fa: KIO[A])(f: A => KIO[B]): KIO[B] = fa.map(f)

    def raiseError[A](error: Throwable): KIO[A] = Abort.fail(error)

object KyoEffectInstance extends KyoEffectInstance
