package mongo4s.repositories

import rapid.Task

import mongo4s.rapid.RapidStream
import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

import mongo4s.rapid.RapidInstances.given

final class RapidRsBridgeBackendSpec extends RsBridgeBackendSpec[Task, RapidStream]:

  protected def bridgeWith(config: RsBridgeConfig): RsBridge[Task, RapidStream] =
    given RsBridgeConfig = config
    summon

  protected def streamableInt: Streamable[RapidStream, Int] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()

  protected def takeFromStream(stream: RapidStream[Int], n: Int): List[Int] = stream.take(n).toList.sync()

  protected def drainStream(stream: RapidStream[Int]): List[Int] = stream.toList.sync()
