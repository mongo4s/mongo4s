package mongo4s.cats

import fs2.Stream
import cats.effect.kernel.Async
import org.reactivestreams.Publisher
import fs2.interop.reactivestreams.fromPublisher

import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

trait AsyncToBridgeInstance:

  given streamable[F[*], A]: Streamable[CatsStream[F], A] = Streamable.instance

  given asyncBridge[F[*]](using F: Async[F], config: RsBridgeConfig): RsBridge[F, CatsStream[F]] with
    def stream[A](publisher: => Publisher[A])(using Streamable[CatsStream[F], A]): Stream[F, A] =
      Stream.eval(F.delay(publisher)).flatMap(fromPublisher[F, A](_, config.bufferSize))

    def one[A](publisher: => Publisher[A]): F[A]            = stream(publisher).take(1).compile.lastOrError
    def option[A](publisher: => Publisher[A]): F[Option[A]] = stream(publisher).take(1).compile.last
    def list[A](publisher: => Publisher[A]): F[List[A]]     = stream(publisher).compile.toList
    def unit[A](publisher: => Publisher[A]): F[Unit]        = stream(publisher).compile.drain

object AsyncToBridgeInstance extends AsyncToBridgeInstance
