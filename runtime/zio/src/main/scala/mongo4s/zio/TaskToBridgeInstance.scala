package mongo4s.zio

import zio.{Duration, Task, ZIO}
import zio.stream.ZStream
import zio.interop.reactivestreams.publisherToStream
import org.reactivestreams.Publisher

import mongo4s.internal.{PublisherCollector, RsBridgeSupport}
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait TaskToBridgeInstance:

  given streamable[A]: Streamable[ZioStream, A] = Streamable.instance

  given taskBridge(using config: RsBridgeConfig): RsBridge[Task, ZioStream] with
    private def withTimeout[A](task: Task[A]): Task[A] = config.timeout match
      case Some(d) => task.timeoutFail(RsBridgeError.Timeout(d))(Duration.fromScala(d))
      case None    => task

    def list[A](publisher: => Publisher[A]): Task[List[A]] =
      withTimeout(ZIO.fromCompletableFuture(PublisherCollector.collect(publisher)))

    def one[A](publisher: => Publisher[A]): Task[A] =
      list(publisher).flatMap(xs => ZIO.fromEither(RsBridgeSupport.selectOne(xs, config.strictSingleResult)))

    def option[A](publisher: => Publisher[A]): Task[Option[A]] =
      list(publisher).flatMap(xs => ZIO.fromEither(RsBridgeSupport.selectOption(xs, config.strictSingleResult)))

    def unit[A](publisher: => Publisher[A]): Task[Unit] = list(publisher).unit

    def stream[A](publisher: => Publisher[A])(using Streamable[ZioStream, A]): ZStream[Any, Throwable, A] =
      ZStream.unwrap(ZIO.attempt(publisher).map(publisherToStream(_).toZIOStream(config.bufferSize)))

object TaskToBridgeInstance extends TaskToBridgeInstance
