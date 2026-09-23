package mongo4s.repositories

import rapid.Task

import mongo4s.rapid.RapidStream
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.rapid.RapidInstances.given

final class RapidRepositoryBackendSpec extends RepositoryBackendSpec[Task, RapidStream]:
  protected def effectInstance: Effect[Task] = summon

  protected def streamable: Streamable[RapidStream, RepositoryBackendSpec.Person] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()

  protected def drain(stream: RapidStream[RepositoryBackendSpec.Person]): List[RepositoryBackendSpec.Person] = stream.toList.sync()

  protected def bridgeInstance: RsBridge[Task, RapidStream] = summon
