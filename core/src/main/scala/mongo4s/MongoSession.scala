package mongo4s

import com.mongodb.reactivestreams.client.ClientSession

object MongoSession:

  def startTransaction[F[*]](session: ClientSession)(using F: Effect[F]): F[Unit] =
    F.delay {
      session.startTransaction()
    }

  def commitTransaction[F[*], S[*]](session: ClientSession)(using rs: RsBridge[F, S]): F[Unit] =
    rs.unit {
      session.commitTransaction()
    }

  def abortTransaction[F[*], S[*]](session: ClientSession)(using rs: RsBridge[F, S]): F[Unit] =
    rs.unit {
      session.abortTransaction()
    }

  private[mongo4s] def abortQuietly[F[*], S[*]](session: ClientSession, cause: Option[Throwable])(using
      F: Effect[F],
      rs: RsBridge[F, S],
  ): F[Unit] =
    F.handleErrorWith(abortTransaction[F, S](session)) { abortError =>
      cause match
        case Some(original) if original ne abortError => F.delay(original.addSuppressed(abortError))
        case _                                        => F.unit
    }

extension (session: ClientSession)
  def withTransaction[F[*], S[*], A](
      fa: Option[ClientSession] ?=> F[A]
  )(using F: Effect[F], rs: RsBridge[F, S]): F[A] =
    given Option[ClientSession] = Some(session)

    F.flatMap(MongoSession.startTransaction[F](session)) { _ =>
      val bodyThenCommit = F.flatMap(fa) { a =>
        F.map(MongoSession.commitTransaction[F, S](session))(_ => a)
      }

      F.guaranteeCase(bodyThenCommit) {
        case ExitCase.Succeeded      => F.unit
        case ExitCase.Errored(error) => MongoSession.abortQuietly[F, S](session, Some(error))
        case ExitCase.Canceled       => MongoSession.abortQuietly[F, S](session, None)
      }
    }
  end withTransaction
