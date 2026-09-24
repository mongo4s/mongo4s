package mongo4s.operations

private[mongo4s] object Bounds:

  def atLeastOne(value: Int, name: String, zeroMeans: String): Int =
    require(value > 0, s"$name must be at least 1, got $value — $zeroMeans")
    value

  def notNegative(value: Int, name: String): Int =
    require(value >= 0, s"$name must not be negative, got $value — MongoDB refuses it")
    value

  def notNegative(value: Double, name: String): Double =
    require(value >= 0, s"$name must not be negative, got $value — MongoDB refuses it")
    value

  val NoLimit    = "MongoDB reads 0 as no limit at all, and a negative limit as a single batch"
  val NoBatching = "MongoDB reads 0 as its own default, and a negative batch size as a single batch that closes the cursor"
