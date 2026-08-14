package mongo4s.kyo

import kyo.*
import kyo.interop.reactivestreams.fromPublisher
import org.reactivestreams.Publisher

import mongo4s.internal.PublisherCollector
import mongo4s.{RsBridge, RsBridgeConfig, Streamable}

trait KyoBridgeInstance:

  given [A] => (emitTag: Tag[Emit[Chunk[A]]]) => (pollTag: Tag[Poll[Chunk[A]]]) => Streamable[KStream, A] =
    KyoBridgeInstance.KyoStreamable(emitTag, pollTag)

  given (config: RsBridgeConfig) => RsBridge[KIO, KStream] = new RsBridge[KIO, KStream]:
    def list[A](publisher: => Publisher[A]): KIO[List[A]] =
      Abort.catching(Async.fromCompletableFuture(PublisherCollector.collect(publisher)))

    def one[A](publisher: => Publisher[A]): KIO[A]            = list(publisher).map(_.head)
    def option[A](publisher: => Publisher[A]): KIO[Option[A]] = list(publisher).map(_.headOption)
    def unit[A](publisher: => Publisher[A]): KIO[Unit]        = list(publisher).map(_ => ())

    def stream[A](publisher: => Publisher[A])(using ev: Streamable[KStream, A]): KStream[A] =
      val streamable            = ev.asInstanceOf[KyoBridgeInstance.KyoStreamable[A]]
      given Tag[Emit[Chunk[A]]] = streamable.emitTag
      given Tag[Poll[Chunk[A]]] = streamable.pollTag
      Stream.unwrap(fromPublisher(publisher, config.bufferSize)).handle(Scope.run)

object KyoBridgeInstance extends KyoBridgeInstance:
  private[kyo] final case class KyoStreamable[A](
      emitTag: Tag[Emit[Chunk[A]]],
      pollTag: Tag[Poll[Chunk[A]]],
  ) extends Streamable[KStream, A]
