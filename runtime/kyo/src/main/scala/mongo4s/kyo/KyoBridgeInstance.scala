package mongo4s.kyo

import kyo.*
import kyo.Sync
import kyo.interop.reactivestreams.fromPublisher
import org.reactivestreams.Publisher

import mongo4s.internal.{PublisherCollector, RsBridgeSupport}
import mongo4s.{RsBridge, RsBridgeConfig, RsBridgeError, Streamable}

trait KyoBridgeInstance:

  given [A] => (emitTag: Tag[Emit[Chunk[A]]]) => (pollTag: Tag[Poll[Chunk[A]]]) => Streamable[KStream, A] =
    KyoBridgeInstance.KyoStreamable(emitTag, pollTag)

  given (config: RsBridgeConfig) => RsBridge[KIO, KStream] = new RsBridge[KIO, KStream]:
    private def liftEither[A](either: Either[RsBridgeError, A]): KIO[A] =
      either match
        case Right(a)    => a
        case Left(error) => Abort.fail[Throwable](error)

    private def withTimeout[A](kio: KIO[A]): KIO[A] =
      config.timeout match
        case Some(d) => Async.timeoutWithError(Duration.fromScala(d), Result.Failure(RsBridgeError.Timeout(d)))(kio)
        case None    => kio

    private def collect[A](publisher: => Publisher[A], limit: Int): KIO[List[A]] =
      withTimeout(
        Sync.defer(
          Abort.catching(
            Async.fromCompletableFuture(PublisherCollector.collect(publisher, limit))
          )
        )
      )

    def list[A](publisher: => Publisher[A]): KIO[List[A]] = collect(publisher, Int.MaxValue)

    def one[A](publisher: => Publisher[A]): KIO[A] =
      collect(publisher, RsBridgeSupport.SingleResultProbe)
        .flatMap(xs => liftEither(RsBridgeSupport.selectOne(xs, config.strictSingleResult)))

    def option[A](publisher: => Publisher[A]): KIO[Option[A]] =
      collect(publisher, RsBridgeSupport.SingleResultProbe)
        .flatMap(xs => liftEither(RsBridgeSupport.selectOption(xs, config.strictSingleResult)))

    def unit[A](publisher: => Publisher[A]): KIO[Unit] =
      withTimeout(
        Sync.defer(
          Abort.catching(
            Async.fromCompletableFuture(PublisherCollector.drain(publisher))
          )
        )
      )

    def stream[A](publisher: => Publisher[A])(using ev: Streamable[KStream, A]): KStream[A] =
      val streamable = ev match
        case derived: KyoBridgeInstance.KyoStreamable[A @unchecked] => derived
        case _                                                      =>
          throw UnsupportedOperationException(
            "mongo4s-kyo streams need the Streamable it derives itself, which carries the kyo Tag for the element " +
              "type; Streamable.instance carries no Tag and cannot drive a kyo Stream. Call the streaming method at " +
              "a concrete element type so the given can be derived."
          )

      given Tag[Emit[Chunk[A]]] = streamable.emitTag
      given Tag[Poll[Chunk[A]]] = streamable.pollTag

      Stream.unwrap(fromPublisher(publisher, config.bufferSize)).handle(Scope.run)
    end stream

object KyoBridgeInstance extends KyoBridgeInstance:
  private[kyo] final case class KyoStreamable[A](
      emitTag: Tag[Emit[Chunk[A]]],
      pollTag: Tag[Poll[Chunk[A]]],
  ) extends Streamable[KStream, A]
