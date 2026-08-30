package mongo4s.repositories

import zio.{Runtime, Task, Unsafe}

import mongo4s.zio.ZioStream
import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

import mongo4s.zio.ZioInstances.given

final class ZioRsBridgeBackendSpec extends RsBridgeBackendSpec[Task, ZioStream]:

  protected def bridgeWith(config: RsBridgeConfig): RsBridge[Task, ZioStream] =
    given RsBridgeConfig = config
    summon

  protected def streamableInt: Streamable[ZioStream, Int] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())

  protected def takeFromStream(stream: ZioStream[Int], n: Int): List[Int] =
    run(stream.take(n.toLong).runCollect.map(_.toList))

  protected def drainStream(stream: ZioStream[Int]): List[Int] = run(stream.runCollect.map(_.toList))
