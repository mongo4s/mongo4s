package mongo4s.repositories

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Stream, Sync as KyoSync}
import mongo4s.{Effect, Streamable}
import mongo4s.kyo.KyoInstances.given
import mongo4s.kyo.{KIO, KStream}

import scala.concurrent.duration.*

final class KyoRepositoryBackendSpec extends RepositoryBackendSpec[KIO, KStream]:
  protected def effectInstance: Effect[KIO] = summon

  protected def streamable: Streamable[KStream, RepositoryBackendSpec.Person] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  protected def drain(stream: KStream[RepositoryBackendSpec.Person]): List[RepositoryBackendSpec.Person] = run(stream.run).toList

  protected def emitStream(values: List[RepositoryBackendSpec.Person]): KStream[RepositoryBackendSpec.Person] = Stream.init(values)
