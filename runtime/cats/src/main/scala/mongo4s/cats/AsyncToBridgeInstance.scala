package mongo4s.cats

import fs2.Stream
import cats.effect.kernel.Async
import org.reactivestreams.Publisher
import fs2.interop.reactivestreams.fromPublisher

import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}
import mongo4s.internal.RsBridgeSupport

trait AsyncToBridgeInstance:

  given streamable[F[*], A]: Streamable[CatsStream[F], A] = Streamable.instance

  given asyncBridge[F[*]](using F: Async[F], config: RsBridgeConfig): RsBridge[F, CatsStream[F]] with
    private def withTimeout[A](fa: F[A]): F[A] = config.timeout match
      case Some(d) => F.timeoutTo(fa, d, F.raiseError(RsBridgeError.Timeout(d)))
      case None    => fa

    def stream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream.eval(F.delay(publisher)).flatMap(fromPublisher[F, A](_, config.bufferSize))

    def one[A](publisher: => Publisher[A]): F[A] =
      withTimeout {
        F.flatMap(stream(publisher).take(2).compile.toList) { xs =>
          RsBridgeSupport.selectOne(xs, config.strictSingleResult).fold(F.raiseError, F.pure)
        }
      }

    def option[A](publisher: => Publisher[A]): F[Option[A]] =
      withTimeout {
        F.flatMap(stream(publisher).take(2).compile.toList) { xs =>
          RsBridgeSupport.selectOption(xs, config.strictSingleResult).fold(F.raiseError, F.pure)
        }
      }

    def list[A](publisher: => Publisher[A]): F[List[A]] = withTimeout(stream(publisher).compile.toList)
    def unit[A](publisher: => Publisher[A]): F[Unit]    = withTimeout(stream(publisher).compile.drain)

object AsyncToBridgeInstance extends AsyncToBridgeInstance
