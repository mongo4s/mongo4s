package mongo4s.repositories

import zio.{Runtime, Task, Unsafe}

import mongo4s.zio.ZioStream
import mongo4s.{Effect, RsBridge}

import mongo4s.zio.ZioInstances.given

final class ZioTransactionBackendSpec extends TransactionBackendSpec[Task, ZioStream]:
  protected def effectInstance: Effect[Task] = summon

  protected def bridgeInstance: RsBridge[Task, ZioStream] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())
