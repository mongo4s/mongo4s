package mongo4s.repositories

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.Effect
import mongo4s.kyo.KIO

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

final class KyoEffectBackendSpec extends EffectBackendSpec[KIO]:
  protected def effectInstance: Effect[KIO] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))
