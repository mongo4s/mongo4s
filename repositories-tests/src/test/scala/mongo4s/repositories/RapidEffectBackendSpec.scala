package mongo4s.repositories

import rapid.Task

import mongo4s.Effect

import mongo4s.rapid.RapidInstances.given

final class RapidEffectBackendSpec extends EffectBackendSpec[Task]:
  protected def effectInstance: Effect[Task] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()
