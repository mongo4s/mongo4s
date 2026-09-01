package mongo4s.rapid

import java.util.concurrent.TimeoutException

import scala.util.{Failure, Success}

import rapid.{Stream, Task}
import org.reactivestreams.Publisher

import mongo4s.internal.{PublisherIterator, RsBridgeSupport}
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait TaskToBridgeInstance:

  given [A] => Streamable[RapidStream, A] = Streamable.instance

  given (config: RsBridgeConfig) => RsBridge[Task, RapidStream] = new RsBridge[Task, RapidStream]:

    private def withTimeout[A](task: Task[A]): Task[A] =
      config.timeout match
        case Some(d) =>
          task.attempt
            .timeout(d)
            .handleError {
              case _: TimeoutException => Task.error(RsBridgeError.Timeout(d))
              case other               => Task.error(other)
            }
            .flatMap {
              case Success(value) => Task.pure(value)
              case Failure(error) => Task.error(error)
            }
        case None    => task

    private def liftEither[A](either: Either[RsBridgeError, A]): Task[A] =
      either match
        case Right(a)    => Task.pure(a)
        case Left(error) => Task.error(error)

    def stream[A](publisher: => Publisher[A])(using Streamable[RapidStream, A]): Stream[A] =
      Stream.fromIteratorManaged[A](Task(PublisherIterator(publisher, config.bufferSize))) {
        case iterator: PublisherIterator[?] => Task(iterator.cancel())
        case _                              => Task.pure(())
      }

    def list[A](publisher: => Publisher[A]): Task[List[A]] = withTimeout(stream(publisher).toList)

    def unit[A](publisher: => Publisher[A]): Task[Unit] = withTimeout(stream(publisher).drain)

    def one[A](publisher: => Publisher[A]): Task[A] =
      withTimeout(
        probe(publisher).flatMap { xs =>
          liftEither(
            RsBridgeSupport.selectOne(xs, config.strictSingleResult)
          )
        }
      )

    def option[A](publisher: => Publisher[A]): Task[Option[A]] =
      withTimeout(
        probe(publisher).flatMap { xs =>
          liftEither(RsBridgeSupport.selectOption(xs, config.strictSingleResult))
        }
      )

    private def probe[A](publisher: => Publisher[A]): Task[List[A]] =
      stream(publisher).take(RsBridgeSupport.SingleResultProbe).toList

object TaskToBridgeInstance extends TaskToBridgeInstance
