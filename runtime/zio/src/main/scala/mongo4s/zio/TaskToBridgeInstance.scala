package mongo4s.zio

import zio.stream.ZStream
import zio.{Duration, Task, ZIO}
import zio.interop.reactivestreams.publisherToStream
import org.reactivestreams.Publisher

import mongo4s.internal.RsBridgeSupport
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait TaskToBridgeInstance:

  given streamable[A]: Streamable[ZioStream, A] = Streamable.instance

  given taskBridge(using config: RsBridgeConfig): RsBridge[Task, ZioStream] with
    private def withTimeout[A](task: Task[A]): Task[A] =
      config.timeout match
        case Some(d) => task.timeoutFail(RsBridgeError.Timeout(d))(Duration.fromScala(d))
        case None    => task

    private def sourceOf[A](publisher: => Publisher[A]): ZStream[Any, Throwable, A] =
      ZStream.unwrap(
        ZIO.attempt(publisher).map { publisher =>
          publisherToStream(publisher).toZIOStream(config.bufferSize)
        }
      )

    def list[A](publisher: => Publisher[A]): Task[List[A]] =
      withTimeout(sourceOf(publisher).runCollect.map(_.toList))

    def unit[A](publisher: => Publisher[A]): Task[Unit] =
      withTimeout(sourceOf(publisher).runDrain)

    def one[A](publisher: => Publisher[A]): Task[A] =
      withTimeout(
        probe(publisher).flatMap { xs =>
          ZIO.fromEither(
            RsBridgeSupport.selectOne(xs, config.strictSingleResult)
          )
        }
      )

    def option[A](publisher: => Publisher[A]): Task[Option[A]] =
      withTimeout(
        probe(publisher).flatMap { xs =>
          ZIO.fromEither(RsBridgeSupport.selectOption(xs, config.strictSingleResult))
        }
      )

    private def probe[A](publisher: => Publisher[A]): Task[List[A]] =
      sourceOf(publisher).take(RsBridgeSupport.SingleResultProbe).runCollect.map(_.toList)

    def stream[A](publisher: => Publisher[A])(using Streamable[ZioStream, A]): ZStream[Any, Throwable, A] =
      sourceOf(publisher)

object TaskToBridgeInstance extends TaskToBridgeInstance
