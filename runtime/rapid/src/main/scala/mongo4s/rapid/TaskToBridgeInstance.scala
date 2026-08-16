package mongo4s.rapid

import java.util.concurrent.TimeoutException

import scala.collection.mutable.ListBuffer

import rapid.{Stream, Task}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.internal.RsBridgeSupport
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait TaskToBridgeInstance:

  given [A] => Streamable[RapidStream, A] = Streamable.instance

  given (config: RsBridgeConfig) => RsBridge[Task, RapidStream] = new RsBridge[Task, RapidStream]:

    private def withTimeout[A](task: Task[A]): Task[A] = config.timeout match
      case Some(d) =>
        task.timeout(d).handleError {
          case _: TimeoutException => Task.error(RsBridgeError.Timeout(d))
          case other               => Task.error(other)
        }
      case None    => task

    private def liftEither[A](either: Either[RsBridgeError, A]): Task[A] = either match
      case Right(a)    => Task.pure(a)
      case Left(error) => Task.error(error)

    def one[A](publisher: => Publisher[A]): Task[A] =
      list(publisher).flatMap(xs => liftEither(RsBridgeSupport.selectOne(xs, config.strictSingleResult)))

    def option[A](publisher: => Publisher[A]): Task[Option[A]] =
      list(publisher).flatMap(xs => liftEither(RsBridgeSupport.selectOption(xs, config.strictSingleResult)))

    def unit[A](publisher: => Publisher[A]): Task[Unit]                                    = list(publisher).map(_ => ())
    def stream[A](publisher: => Publisher[A])(using Streamable[RapidStream, A]): Stream[A] = Stream.force(list(publisher).map(Stream.emits))

    def list[A](publisher: => Publisher[A]): Task[List[A]] =
      withTimeout {
        Task.defer {
          val completable = Task.completable[List[A]]
          val buffer      = ListBuffer.empty[A]

          publisher.subscribe(
            new Subscriber[A]:
              def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue)
              def onNext(value: A): Unit                        = buffer += value
              def onError(error: Throwable): Unit               = completable.failure(error)
              def onComplete(): Unit                            = completable.success(buffer.toList)
          )
          completable
        }
      }

object TaskToBridgeInstance extends TaskToBridgeInstance
