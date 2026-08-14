package mongo4s

import mongo4s.bson.BsonError

trait Effect[F[*]]:
  def pure[A](a: A): F[A]
  def delay[A](a: => A): F[A]
  def map[A, B](fa: F[A])(f: A => B): F[B]
  def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
  def raiseError[A](error: Throwable): F[A]

object Effect:
  inline def apply[F[*]](using F: Effect[F]): F.type = F

  def fromEither[F[*], A](either: Either[BsonError, A])(using F: Effect[F]): F[A] =
    either.fold(error => F.raiseError(error.toThrowable), F.pure)

  def traverse[F[*], A, B](values: List[A])(f: A => F[List[B]])(using F: Effect[F]): F[List[B]] =
    values.foldLeft(F.pure(List.empty[B])): (acc, value) =>
      F.flatMap(acc)(collected => F.map(f(value))(collected ++ _))

  def traverse_[F[*], A](values: List[A])(f: A => F[Unit])(using F: Effect[F]): F[Unit] =
    values.foldLeft(F.pure(())): (acc, value) =>
      F.flatMap(acc)(_ => f(value))

  def whenA[F[*]](condition: Boolean)(action: => F[Unit])(using F: Effect[F]): F[Unit] =
    if condition then action else F.pure(())

  extension [F[*], A](fa: F[A])(using F: Effect[F])
    def mapF[B](f: A => B): F[B]        = F.map(fa)(f)
    def flatMapF[B](f: A => F[B]): F[B] = F.flatMap(fa)(f)
    def voidF: F[Unit]                  = F.map(fa)(_ => ())
