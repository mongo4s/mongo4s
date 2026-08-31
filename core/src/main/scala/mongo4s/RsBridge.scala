package mongo4s

import org.reactivestreams.Publisher

trait RsBridge[F[*], S[*]]:
  def one[A](publisher: => Publisher[A]): F[A]
  def option[A](publisher: => Publisher[A]): F[Option[A]]
  def list[A](publisher: => Publisher[A]): F[List[A]]
  def unit[A](publisher: => Publisher[A]): F[Unit]
  def stream[A](publisher: => Publisher[A])(using Streamable[S, A]): S[A]

  def liveStream[A](publisher: => Publisher[A])(using Streamable[S, A]): S[A] = stream(publisher)

object RsBridge:
  inline def apply[F[*], S[*]](using instance: RsBridge[F, S]): instance.type = instance
