package mongo4s.it

import zio.{Runtime, Task, Unsafe}

import mongo4s.{Effect, RsBridge, Streamable}
import mongo4s.zio.ZioInstances.given
import mongo4s.zio.ZioStream

import DirectRepositoryItSpec.Person

final class ZioDirectRepositoryItSpec extends DirectRepositoryItSpec[Task, ZioStream]:
  protected def effectInstance: Effect[Task]              = summon
  protected def rsBridge: RsBridge[Task, ZioStream]       = summon
  protected def streamable: Streamable[ZioStream, Person] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())

  protected def drain(stream: ZioStream[Person]): List[Person] = run(stream.runCollect.map(_.toList))
