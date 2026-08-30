package mongo4s.repositories

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.{Effect, Streamable}

import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given

final class CatsRepositoryBackendSpec extends RepositoryBackendSpec[IO, CatsStream[IO]]:
  protected def effectInstance: Effect[IO] = summon

  protected def streamable: Streamable[CatsStream[IO], RepositoryBackendSpec.Person] = summon

  protected def run[A](fa: IO[A]): A = fa.unsafeRunSync()

  protected def drain(stream: CatsStream[IO][RepositoryBackendSpec.Person]): List[RepositoryBackendSpec.Person] =
    stream.compile.toList.unsafeRunSync()

  protected def emitStream(values: List[RepositoryBackendSpec.Person]): CatsStream[IO][RepositoryBackendSpec.Person] =
    fs2.Stream.emits(values).covary[IO]
