package mongo4s.it.watch

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.kyo.{KIO, KStream}
import mongo4s.changestream.ChangeEvent
import mongo4s.{Effect, RsBridge, Streamable}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

import WatchFixture.Person

final class KyoRuntimeWatchItSpec extends RuntimeWatchItSpec[KIO, KStream]:
  protected def runtimeName: String = "kyo"

  protected def effectInstance: Effect[KIO]                          = summon
  protected def rsBridge: RsBridge[KIO, KStream]                     = summon
  protected def streamable: Streamable[KStream, ChangeEvent[Person]] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(60.seconds))(fa))

  protected def takeEvents(stream: KStream[ChangeEvent[Person]], n: Int): List[ChangeEvent[Person]] =
    run(stream.take(n).run).toList
