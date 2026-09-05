package mongo4s.repositories

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.kyo.{KIO, KStream}
import mongo4s.{Effect, RsBridge}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

final class KyoTransactionBackendSpec extends TransactionBackendSpec[KIO, KStream]:
  protected def effectInstance: Effect[KIO] = summon

  protected def bridgeInstance: RsBridge[KIO, KStream] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))
