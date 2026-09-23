package mongo4s

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import scala.concurrent.duration.FiniteDuration

import com.mongodb.MongoException
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.TransactionOptions

object MongoSession:

  private val BackoffInitial = FiniteDuration(5, TimeUnit.MILLISECONDS)
  private val BackoffMax     = FiniteDuration(500, TimeUnit.MILLISECONDS)
  private val BackoffGrowth  = 1.5

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

  private[mongo4s] def exceededMaxTime(error: Throwable): Boolean =
    MongoError.translate(error) match
      case _: MongoError.ExecutionTimeout => true
      case _                              => false

  private[mongo4s] def backoff[F[*]](attempt: Int)(using F: Effect[F]): F[FiniteDuration] =
    F.delay {
      val ceiling = math.min(BackoffInitial.toNanos * math.pow(BackoffGrowth, attempt.toDouble), BackoffMax.toNanos.toDouble)
      FiniteDuration((ThreadLocalRandom.current().nextDouble() * ceiling).toLong, TimeUnit.NANOSECONDS)
    }

end MongoSession
