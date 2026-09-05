package mongo4s.repositories

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.{Effect, RsBridge}

import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given

final class CatsTransactionBackendSpec extends TransactionBackendSpec[IO, CatsStream[IO]]:
  protected def effectInstance: Effect[IO] = summon

  protected def bridgeInstance: RsBridge[IO, CatsStream[IO]] = summon

  protected def run[A](fa: IO[A]): A = fa.unsafeRunSync()
