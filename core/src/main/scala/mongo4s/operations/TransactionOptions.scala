package mongo4s.operations

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import com.mongodb.{ReadConcern, ReadPreference, WriteConcern, TransactionOptions as DriverTransactionOptions}

final class TransactionOptions private (
    val readConcern: Option[ReadConcern],
    val writeConcern: Option[WriteConcern],
    val readPreference: Option[ReadPreference],
    val maxCommitTime: Option[FiniteDuration],
    val retryTimeout: Option[FiniteDuration],
):
  def withReadConcern(value: ReadConcern): TransactionOptions = copy(readConcern = Some(value))

  def withWriteConcern(value: WriteConcern): TransactionOptions = copy(writeConcern = Some(value))

  def withReadPreference(value: ReadPreference): TransactionOptions = copy(readPreference = Some(value))

  def withMaxCommitTime(value: FiniteDuration): TransactionOptions = copy(maxCommitTime = Some(value))

  def withRetryTimeout(value: FiniteDuration): TransactionOptions = copy(retryTimeout = Some(value))

  def withoutRetries: TransactionOptions = copy(retryTimeout = None)

  def toDriver: DriverTransactionOptions =
    val builder = DriverTransactionOptions.builder()

    readConcern.foreach(builder.readConcern)
    writeConcern.foreach(builder.writeConcern)
    readPreference.foreach(builder.readPreference)
    maxCommitTime.foreach(value => builder.maxCommitTime(value.toMillis, TimeUnit.MILLISECONDS))

    builder.build()
  end toDriver

  private def copy(
      readConcern: Option[ReadConcern] = readConcern,
      writeConcern: Option[WriteConcern] = writeConcern,
      readPreference: Option[ReadPreference] = readPreference,
      maxCommitTime: Option[FiniteDuration] = maxCommitTime,
      retryTimeout: Option[FiniteDuration] = retryTimeout,
  ): TransactionOptions =
    new TransactionOptions(
      readConcern = readConcern,
      writeConcern = writeConcern,
      readPreference = readPreference,
      maxCommitTime = maxCommitTime,
      retryTimeout = retryTimeout,
    )

object TransactionOptions:
  val DefaultRetryTimeout: FiniteDuration = FiniteDuration(120, TimeUnit.SECONDS)

  val default: TransactionOptions =
    new TransactionOptions(
      readConcern = None,
      writeConcern = None,
      readPreference = None,
      maxCommitTime = None,
      retryTimeout = Some(DefaultRetryTimeout),
    )

  val withoutRetries: TransactionOptions = default.withoutRetries
