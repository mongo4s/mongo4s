package mongo4s.repositories

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

import scala.concurrent.duration.given
import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given

final class CatsRsBridgeBackendSpec extends RsBridgeBackendSpec[IO, CatsStream[IO]]:

  protected def bridgeWith(config: RsBridgeConfig): RsBridge[IO, CatsStream[IO]] =
    given RsBridgeConfig = config
    summon

  protected def streamableInt: Streamable[CatsStream[IO], Int] = summon

  protected def run[A](fa: IO[A]): A =
    fa.unsafeRunTimed(60.seconds).getOrElse(throw RuntimeException("effect did not complete within 60s"))

  protected def takeFromStream(stream: CatsStream[IO][Int], n: Int): List[Int] =
    stream.take(n.toLong).compile.toList.unsafeRunSync()

  protected def drainStream(stream: CatsStream[IO][Int]): List[Int] = stream.compile.toList.unsafeRunSync()
