package mongo4s.it.watch

import rapid.Task

import mongo4s.rapid.RapidStream
import mongo4s.changestream.ChangeEvent
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.rapid.RapidInstances.given

import WatchFixture.Person

final class RapidRuntimeWatchItSpec extends RuntimeWatchItSpec[Task, RapidStream]:
  protected def runtimeName: String = "rapid"

  protected def effectInstance: Effect[Task]                             = summon
  protected def rsBridge: RsBridge[Task, RapidStream]                    = summon
  protected def streamable: Streamable[RapidStream, ChangeEvent[Person]] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()

  protected def takeEvents(stream: RapidStream[ChangeEvent[Person]], n: Int): List[ChangeEvent[Person]] =
    stream.take(n).toList.sync()
