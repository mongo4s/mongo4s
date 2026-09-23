package mongo4s.operations

import com.mongodb.ClientSessionOptions

final class SessionOptions private (
    val causallyConsistent: Option[Boolean],
    val snapshot: Boolean,
    val defaultTransactionOptions: Option[TransactionOptions],
):
  def withCausalConsistency(value: Boolean): SessionOptions =
    require(!(value && snapshot), "a snapshot session cannot also be causally consistent — the server refuses the combination")
    copy(causallyConsistent = Some(value))
  end withCausalConsistency

  def withSnapshot: SessionOptions =
    require(!causallyConsistent.contains(true), "a causally consistent session cannot also read from a snapshot — the server refuses the combination")
    copy(snapshot = true)
  end withSnapshot

  def withDefaultTransactionOptions(value: TransactionOptions): SessionOptions = copy(defaultTransactionOptions = Some(value))

  def toDriver: ClientSessionOptions =
    val builder = ClientSessionOptions.builder().snapshot(snapshot)

    causallyConsistent.foreach(value => builder.causallyConsistent(value))
    defaultTransactionOptions.foreach(value => builder.defaultTransactionOptions(value.toDriver))

    builder.build()
  end toDriver

  private def copy(
      causallyConsistent: Option[Boolean] = causallyConsistent,
      snapshot: Boolean = snapshot,
      defaultTransactionOptions: Option[TransactionOptions] = defaultTransactionOptions,
  ): SessionOptions =
    new SessionOptions(
      causallyConsistent = causallyConsistent,
      snapshot = snapshot,
      defaultTransactionOptions = defaultTransactionOptions,
    )

object SessionOptions:
  val default: SessionOptions =
    new SessionOptions(
      causallyConsistent = None,
      snapshot = false,
      defaultTransactionOptions = None,
    )

  val snapshot: SessionOptions = default.withSnapshot
