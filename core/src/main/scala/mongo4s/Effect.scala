package mongo4s

import mongo4s.bson.BsonError

trait Effect[F[*]]:
  def pure[A](a: A): F[A]
  def delay[A](a: => A): F[A]
  def map[A, B](fa: F[A])(f: A => B): F[B]
  def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
  def raiseError[A](error: Throwable): F[A]
  def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A]
  def guaranteeCase[A](fa: F[A])(finalizer: ExitCase => F[Unit]): F[A]

  protected final def runFinalizer(exitCase: ExitCase, finalizer: ExitCase => F[Unit]): F[Unit] =
    handleErrorWith(finalizer(exitCase)) { finalizerError =>
      exitCase match
        case ExitCase.Succeeded         => raiseError(finalizerError)
        case ExitCase.Canceled          => unit
        case ExitCase.Errored(original) =>
          if original ne finalizerError
          then delay(original.addSuppressed(finalizerError))
          else unit
    }

  def suspend[A](fa: => F[A]): F[A]                    = flatMap(delay(()))(_ => fa)
  def unit: F[Unit]                                    = pure(())
  def void[A](fa: F[A]): F[Unit]                       = map(fa)(_ => ())
  def attempt[A](fa: F[A]): F[Either[Throwable, A]]    = handleErrorWith(map(fa)(a => Right(a): Either[Throwable, A]))(error => pure(Left(error)))
  def guarantee[A](fa: F[A])(finalizer: F[Unit]): F[A] = guaranteeCase(fa)(_ => finalizer)

  def onError[A](fa: F[A])(f: Throwable => F[Unit]): F[A] =
    guaranteeCase(fa) {
      case ExitCase.Succeeded      => unit
      case ExitCase.Canceled       => unit
      case ExitCase.Errored(error) => f(error)
    }

  def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase) => F[Unit]): F[B] =
    flatMap(acquire)(a => guaranteeCase(use(a))(exitCase => release(a, exitCase)))

  def bracket[A, B](acquire: F[A])(use: A => F[B])(release: A => F[Unit]): F[B] =
    bracketCase(acquire)(use)((a, _) => release(a))

object Effect:
  inline def apply[F[*]](using F: Effect[F]): F.type = F

  def fromEither[F[*], A](either: Either[BsonError, A])(using F: Effect[F]): F[A] =
    either.fold(error => F.raiseError(error.toThrowable), F.pure)

  def traverse[F[*], A, B](values: List[A])(f: A => F[List[B]])(using F: Effect[F]): F[List[B]] =
    val reversed = values.foldLeft(F.pure(List.empty[List[B]])) { (acc, value) =>
      F.flatMap(acc)(chunks => F.map(f(value))(_ :: chunks))
    }

    F.map(reversed)(chunks => chunks.reverse.flatten)
  end traverse

  def traverse_[F[*], A](values: List[A])(f: A => F[Unit])(using F: Effect[F]): F[Unit] =
    values.foldLeft(F.pure(())) { (acc, value) =>
      F.flatMap(acc)(_ => f(value))
    }

  def whenA[F[*]](condition: Boolean)(action: => F[Unit])(using F: Effect[F]): F[Unit] =
    if condition
    then action
    else F.pure(())
