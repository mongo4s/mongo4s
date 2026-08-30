package mongo4s.it

import rapid.Task

import mongo4s.rapid.RapidStream
import mongo4s.{Effect, RsBridge, Streamable}

import mongo4s.rapid.RapidInstances.given

import DirectRepositoryItSpec.Person

final class RapidDirectRepositoryItSpec extends DirectRepositoryItSpec[Task, RapidStream]:
  protected def effectInstance: Effect[Task]                = summon
  protected def rsBridge: RsBridge[Task, RapidStream]       = summon
  protected def streamable: Streamable[RapidStream, Person] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()

  protected def drain(stream: RapidStream[Person]): List[Person] = stream.toList.sync()
