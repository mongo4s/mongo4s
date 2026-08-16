package mongo4s.bson.direct

import mongo4s.bson.FieldNaming

final case class WireCodecConfig(
    fieldNaming: FieldNaming = FieldNaming.identity,
    discriminatorNaming: FieldNaming = FieldNaming.identity,
    encodeEmptyCasesAsString: Boolean = false,
)

object WireCodecConfig:
  val Default: WireCodecConfig   = WireCodecConfig()
  val SnakeCase: WireCodecConfig = WireCodecConfig(fieldNaming = FieldNaming.snakeCase)

  given default: WireCodecConfig = Default
