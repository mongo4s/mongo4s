package mongo4s.repositories

import rapid.Task

import mongo4s.rapid.RapidStream
import mongo4s.{Effect, RsBridge}

import mongo4s.rapid.RapidInstances.given

final class RapidTransactionBackendSpec extends TransactionBackendSpec[Task, RapidStream]:
  protected def effectInstance: Effect[Task] = summon

  protected def bridgeInstance: RsBridge[Task, RapidStream] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()
