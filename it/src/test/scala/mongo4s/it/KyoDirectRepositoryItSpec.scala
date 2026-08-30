package mongo4s.it

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.kyo.{KIO, KStream}
import mongo4s.{Effect, RsBridge, Streamable}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

import DirectRepositoryItSpec.Person

final class KyoDirectRepositoryItSpec extends DirectRepositoryItSpec[KIO, KStream]:
  protected def effectInstance: Effect[KIO]             = summon
  protected def rsBridge: RsBridge[KIO, KStream]        = summon
  protected def streamable: Streamable[KStream, Person] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  protected def drain(stream: KStream[Person]): List[Person] = run(stream.run).toList
