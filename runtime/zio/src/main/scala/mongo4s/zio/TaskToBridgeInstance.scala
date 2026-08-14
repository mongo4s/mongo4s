package mongo4s.zio

import zio.{Task, ZIO}
import zio.stream.ZStream
import zio.interop.reactivestreams.publisherToStream
import org.reactivestreams.Publisher

import mongo4s.{RsBridge, RsBridgeConfig, Streamable}
import mongo4s.internal.PublisherCollector

trait TaskToBridgeInstance:

  given streamable[A]: Streamable[ZioStream, A] = Streamable.instance

  given taskBridge(using config: RsBridgeConfig): RsBridge[Task, ZioStream] with
    def list[A](publisher: => Publisher[A]): Task[List[A]] =
      ZIO.fromCompletableFuture(PublisherCollector.collect(publisher))

    def one[A](publisher: => Publisher[A]): Task[A] =
      list(publisher).flatMap(xs => ZIO.getOrFailWith(new NoSuchElementException("Empty publisher"))(xs.headOption))

    def option[A](publisher: => Publisher[A]): Task[Option[A]] = list(publisher).map(_.headOption)
    def unit[A](publisher: => Publisher[A]): Task[Unit]        = list(publisher).unit

    def stream[A](publisher: => Publisher[A])(using Streamable[ZioStream, A]): ZStream[Any, Throwable, A] =
      ZStream.unwrap(ZIO.attempt(publisher).map(publisherToStream(_).toZIOStream(config.bufferSize)))

object TaskToBridgeInstance extends TaskToBridgeInstance
