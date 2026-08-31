package mongo4s.it.watch

import zio.{Runtime, Task, Unsafe}

import mongo4s.zio.ZioStream
import mongo4s.changestream.ChangeEvent
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.zio.ZioInstances.given

import WatchFixture.Person

final class ZioRuntimeWatchItSpec extends RuntimeWatchItSpec[Task, ZioStream]:
  protected def runtimeName: String = "ZIO"

  protected def effectInstance: Effect[Task]                           = summon
  protected def rsBridge: RsBridge[Task, ZioStream]                    = summon
  protected def streamable: Streamable[ZioStream, ChangeEvent[Person]] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())

  protected def takeEvents(stream: ZioStream[ChangeEvent[Person]], n: Int): List[ChangeEvent[Person]] =
    run(stream.take(n.toLong).runCollect.map(_.toList))
