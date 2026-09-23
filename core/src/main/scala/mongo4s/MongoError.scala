package mongo4s

import scala.util.control.NoStackTrace
import scala.jdk.CollectionConverters.given

import com.mongodb.bulk.BulkWriteError
import com.mongodb.{
  ErrorCategory,
  MongoBulkWriteException,
  MongoException,
  MongoExecutionTimeoutException,
  MongoSocketException,
  MongoTimeoutException,
  MongoWriteException,
}

import mongo4s.results.BulkWriteResult

sealed abstract class MongoError(val cause: MongoException) extends RuntimeException(cause.getMessage, cause), NoStackTrace:
  def code: Int = cause.getCode

  def labels: Set[String] = cause.getErrorLabels.asScala.toSet

  def hasLabel(label: String): Boolean = cause.hasErrorLabel(label)

object MongoError:

  private val WriteConflictCode      = 112
  private val UnauthorizedCode       = 13
  private val AuthenticationFailCode = 18

  final case class DuplicateKey(override val cause: MongoException)     extends MongoError(cause)
  final case class WriteConflict(override val cause: MongoException)    extends MongoError(cause)
  final case class ExecutionTimeout(override val cause: MongoException) extends MongoError(cause)
  final case class Unauthorized(override val cause: MongoException)     extends MongoError(cause)
  final case class Unavailable(override val cause: MongoException)      extends MongoError(cause)
  final case class Failed(override val cause: MongoException)           extends MongoError(cause)

  final case class BulkWriteFailed(failures: List[BulkFailure], override val cause: MongoBulkWriteException) extends MongoError(cause):
    def duplicateKeys: List[BulkFailure] = failures.filter(_.isDuplicateKey)

    def result: BulkWriteResult = BulkWriteResult.fromDriver(cause.getWriteResult)

    private[mongo4s] def after(offset: Int, before: BulkWriteResult): BulkWriteFailed =
      val written = BulkWriteResult.combine(List(before, result.shiftUpsertedIds(offset)))
      val errors  = cause.getWriteErrors.asScala.toList.map(e => BulkWriteError(e.getCode, e.getMessage, e.getDetails, e.getIndex + offset))

      bulkWriteFailed(MongoBulkWriteException(written.toDriver, errors.asJava, cause.getWriteConcernError, cause.getServerAddress, cause.getErrorLabels))
    end after

  final case class BulkFailure(index: Int, code: Int, message: String):
    def isDuplicateKey: Boolean = ErrorCategory.fromErrorCode(code) == ErrorCategory.DUPLICATE_KEY

  private[mongo4s] def translate(error: Throwable): Throwable = error match
    case already: MongoError => already

    case bulk: MongoBulkWriteException => bulkWriteFailed(bulk)

    case write: MongoWriteException              => byCategory(write.getError.getCategory, write)
    case timeout: MongoExecutionTimeoutException => ExecutionTimeout(timeout)
    case socket: MongoSocketException            => Unavailable(socket)
    case selection: MongoTimeoutException        => Unavailable(selection)
    case mongo: MongoException                   => byCategory(ErrorCategory.fromErrorCode(mongo.getCode), mongo)
    case other                                   => other

  private[mongo4s] def bulkWriteFailed(bulk: MongoBulkWriteException): BulkWriteFailed =
    BulkWriteFailed(
      bulk.getWriteErrors.asScala.map(e => BulkFailure(e.getIndex, e.getCode, e.getMessage)).toList,
      bulk,
    )

  private def byCategory(category: ErrorCategory, error: MongoException): MongoError = category match
    case ErrorCategory.DUPLICATE_KEY     => DuplicateKey(error)
    case ErrorCategory.EXECUTION_TIMEOUT => ExecutionTimeout(error)
    case ErrorCategory.UNCATEGORIZED     =>
      error.getCode match
        case WriteConflictCode                         => WriteConflict(error)
        case UnauthorizedCode | AuthenticationFailCode => Unauthorized(error)
        case _                                         => Failed(error)
