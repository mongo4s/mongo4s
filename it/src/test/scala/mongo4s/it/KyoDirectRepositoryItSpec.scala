package mongo4s.it

import _root_.kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.{Effect, RsBridge, Streamable}
import mongo4s.kyo.KyoInstances.given
import mongo4s.kyo.{KIO, KStream}

import scala.concurrent.duration.*

import DirectRepositoryItSpec.Person

final class KyoDirectRepositoryItSpec extends DirectRepositoryItSpec[KIO, KStream]:
  protected def effectInstance: Effect[KIO]             = summon
  protected def rsBridge: RsBridge[KIO, KStream]        = summon
  protected def streamable: Streamable[KStream, Person] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  protected def drain(stream: KStream[Person]): List[Person] = run(stream.run).toList
