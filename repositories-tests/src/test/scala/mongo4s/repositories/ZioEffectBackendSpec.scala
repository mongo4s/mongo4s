package mongo4s.repositories

import zio.{Runtime, Task, Unsafe}

import mongo4s.Effect

import mongo4s.zio.ZioInstances.given

final class ZioEffectBackendSpec extends EffectBackendSpec[Task]:
  protected def effectInstance: Effect[Task] = summon

  protected def run[A](fa: Task[A]): A = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())
