package mongo4s.repositories

import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}

import mongo4s.kyo.{KIO, KStream}
import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

final class KyoRsBridgeBackendSpec extends RsBridgeBackendSpec[KIO, KStream]:

  protected def bridgeWith(config: RsBridgeConfig): RsBridge[KIO, KStream] =
    given RsBridgeConfig = config
    summon

  protected def streamableInt: Streamable[KStream, Int] = summon

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  protected def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  protected def takeFromStream(stream: KStream[Int], n: Int): List[Int] = run(stream.take(n).run).toList

  protected def drainStream(stream: KStream[Int]): List[Int] = run(stream.run).toList
