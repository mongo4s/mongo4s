package mongo4s.it.watch

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import mongo4s.cats.CatsStream
import mongo4s.changestream.ChangeEvent
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.cats.CatsInstances.given

import WatchFixture.Person

final class CatsRuntimeWatchItSpec extends RuntimeWatchItSpec[IO, CatsStream[IO]]:
  protected def runtimeName: String = "cats-effect"

  protected def effectInstance: Effect[IO]                                  = summon
  protected def rsBridge: RsBridge[IO, CatsStream[IO]]                      = summon
  protected def streamable: Streamable[CatsStream[IO], ChangeEvent[Person]] = summon

  protected def run[A](fa: IO[A]): A = fa.unsafeRunSync()

  protected def takeEvents(stream: CatsStream[IO][ChangeEvent[Person]], n: Int): List[ChangeEvent[Person]] =
    stream.take(n.toLong).compile.toList.unsafeRunSync()
