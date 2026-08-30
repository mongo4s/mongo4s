package mongo4s.repositories

import cats.effect.IO

import mongo4s.Effect

import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given

final class CatsEffectBackendSpec extends EffectBackendSpec[IO]:
  protected def effectInstance: Effect[IO] = summon

  protected def run[A](fa: IO[A]): A = fa.unsafeRunSync()
