package mongo4s

import com.mongodb.MongoException
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.TransactionOptions

object MongoSession:

  def startTransaction[F[*]](
      session: ClientSession,
      options: TransactionOptions = TransactionOptions.default,
  )(using F: Effect[F]): F[Unit] =
    F.delay {
      session.startTransaction(options.toDriver)
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

  private[mongo4s] def hasErrorLabel(error: Throwable, label: String): Boolean =
    error match
      case mongo: MongoException => mongo.hasErrorLabel(label)
      case typed: MongoError     => typed.hasLabel(label)
      case _                     => false

end MongoSession
