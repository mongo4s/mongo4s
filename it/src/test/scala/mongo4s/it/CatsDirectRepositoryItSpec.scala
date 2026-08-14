package mongo4s.it

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import mongo4s.{Effect, RsBridge, Streamable}
import mongo4s.cats.CatsInstances.given
import mongo4s.cats.CatsStream

import DirectRepositoryItSpec.Person

final class CatsDirectRepositoryItSpec extends DirectRepositoryItSpec[IO, CatsStream[IO]]:
  protected def effectInstance: Effect[IO]                     = summon
  protected def rsBridge: RsBridge[IO, CatsStream[IO]]         = summon
  protected def streamable: Streamable[CatsStream[IO], Person] = summon

  protected def run[A](fa: IO[A]): A = fa.unsafeRunSync()

  protected def drain(stream: CatsStream[IO][Person]): List[Person] = stream.compile.toList.unsafeRunSync()
