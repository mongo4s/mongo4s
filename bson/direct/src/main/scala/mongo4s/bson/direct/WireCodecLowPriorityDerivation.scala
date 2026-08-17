package mongo4s.bson.direct

import scala.deriving.Mirror

private[direct] trait WireCodecLowPriorityDerivation:
  inline given derived[A](using m: Mirror.Of[A], config: WireCodecConfig): WireCodec[A] =
    inline m match
      case p: Mirror.ProductOf[A] => WireProductDerivation.derived[A](using p, config)
      case s: Mirror.SumOf[A]     => WireSumDerivation.derived[A](using s, config)
