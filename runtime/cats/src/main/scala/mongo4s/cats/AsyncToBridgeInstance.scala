package mongo4s.cats

import fs2.Stream
import cats.effect.kernel.Async
import org.reactivestreams.Publisher
import fs2.interop.reactivestreams.fromPublisher

import mongo4s.internal.{PublisherCollector, RsBridgeSupport}
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait AsyncToBridgeInstance:

  given streamable[F[*], A]: Streamable[CatsStream[F], A] = Streamable.instance

  given asyncBridge[F[*]](using F: Async[F], config: RsBridgeConfig): RsBridge[F, CatsStream[F]] with
    private def withTimeout[A](fa: F[A]): F[A] = config.timeout match
      case Some(d) => F.timeoutTo(fa, d, F.raiseError(RsBridgeError.Timeout(d)))
      case None    => fa

    def stream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream
        .eval(F.delay(publisher))
        .flatMap(fromPublisher[F, A](_, config.bufferSize))

    override def liveStream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream
        .eval(F.delay(publisher))
        .flatMap(fromPublisher[F, A](_, 1))

    private def collect[A](publisher: => Publisher[A], limit: Int): F[List[A]] =
      withTimeout(
        F.fromCompletableFuture(
          F.delay(PublisherCollector.collect(publisher, limit))
        )
      )

    def list[A](publisher: => Publisher[A]): F[List[A]] =
      collect(publisher, Int.MaxValue)

    def unit[A](publisher: => Publisher[A]): F[Unit] =
      withTimeout(
        F.void(
          F.fromCompletableFuture(
            F.delay(PublisherCollector.drain(publisher))
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
