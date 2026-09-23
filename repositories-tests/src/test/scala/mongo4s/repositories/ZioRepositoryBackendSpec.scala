package mongo4s.repositories

import zio.{Runtime, Task, Unsafe}

import mongo4s.zio.ZioStream
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.zio.ZioInstances.given

final class ZioRepositoryBackendSpec extends RepositoryBackendSpec[Task, ZioStream]:
  protected def effectInstance: Effect[Task] = summon

  protected def streamable: Streamable[ZioStream, RepositoryBackendSpec.Person] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())

  protected def drain(stream: ZioStream[RepositoryBackendSpec.Person]): List[RepositoryBackendSpec.Person] =
    run(stream.runCollect.map(_.toList))

  protected def bridgeInstance: RsBridge[Task, ZioStream] = summon
