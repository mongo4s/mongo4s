package mongo4s.cats

import fs2.Stream
import cats.effect.kernel.Async
import org.reactivestreams.Publisher
import fs2.interop.reactivestreams.fromPublisher

import mongo4s.internal.{PublisherCollector, RsBridgeSupport}
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait AsyncToBridgeInstance:

  given streamable: [F[*], A] => Streamable[CatsStream[F], A] = Streamable.instance

  given asyncBridge: [F[*]] => (F: Async[F], config: RsBridgeConfig) => RsBridge[F, CatsStream[F]]:
    private def withTimeout[A](fa: F[A]): F[A] = config.timeout match
      case Some(d) => F.timeoutTo(fa, d, F.raiseError(RsBridgeError.Timeout(d)))
      case None    => fa

    private def source[A](publisher: => Publisher[A]): F[Publisher[A]] =
      F.delay(RsBridgeSupport.translating(publisher))

    def stream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream
        .eval(source(publisher))
        .flatMap(fromPublisher[F, A](_, config.bufferSize))

    override def liveStream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream
        .eval(source(publisher))
        .flatMap(fromPublisher[F, A](_, 1))

    private def collect[A](publisher: => Publisher[A], limit: Int): F[List[A]] =
      withTimeout(
        F.fromCompletableFuture(
          F.map(source(publisher))(PublisherCollector.collect(_, limit))
        )
      )

    def list[A](publisher: => Publisher[A]): F[List[A]] =
      collect(publisher, Int.MaxValue)

    def unit[A](publisher: => Publisher[A]): F[Unit] =
      withTimeout(
        F.void(
          F.fromCompletableFuture(
            F.map(source(publisher))(PublisherCollector.drain)
          )
        )
      )

    def one[A](publisher: => Publisher[A]): F[A] =
      F.flatMap(collect(publisher, RsBridgeSupport.SingleResultProbe)) { xs =>
        RsBridgeSupport.selectOne(xs, config.strictSingleResult).fold(F.raiseError, F.pure)
      }

    def option[A](publisher: => Publisher[A]): F[Option[A]] =
      F.flatMap(collect(publisher, RsBridgeSupport.SingleResultProbe)) { xs =>
        RsBridgeSupport.selectOption(xs, config.strictSingleResult).fold(F.raiseError, F.pure)
      }

object AsyncToBridgeInstance extends AsyncToBridgeInstance
