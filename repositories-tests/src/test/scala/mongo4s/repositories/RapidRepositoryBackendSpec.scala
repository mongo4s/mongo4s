package mongo4s.repositories

import mongo4s.{Effect, Streamable}
import mongo4s.rapid.RapidInstances.given
import mongo4s.rapid.RapidStream
import rapid.{Stream, Task}

final class RapidRepositoryBackendSpec extends RepositoryBackendSpec[Task, RapidStream]:
  protected def effectInstance: Effect[Task] = summon

  protected def streamable: Streamable[RapidStream, RepositoryBackendSpec.Person] = summon

  protected def run[A](fa: Task[A]): A = fa.sync()

  protected def drain(stream: RapidStream[RepositoryBackendSpec.Person]): List[RepositoryBackendSpec.Person] = stream.toList.sync()

  protected def emitStream(values: List[RepositoryBackendSpec.Person]): RapidStream[RepositoryBackendSpec.Person] = Stream.emits(values)
