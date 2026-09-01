package mongo4s.bson.direct

import mongo4s.bson.FieldNaming

final class WireCodecConfig private (
    val fieldNaming: FieldNaming,
    val discriminatorNaming: FieldNaming,
    val encodeEmptyCasesAsString: Boolean,
    val omitNoneFields: Boolean,
):
  def withFieldNaming(value: FieldNaming): WireCodecConfig =
    copy(fieldNaming = value)

  def withDiscriminatorNaming(value: FieldNaming): WireCodecConfig =
    copy(discriminatorNaming = value)

  def withEncodeEmptyCasesAsString(value: Boolean): WireCodecConfig =
    copy(encodeEmptyCasesAsString = value)

  def withOmitNoneFields(value: Boolean): WireCodecConfig =
    copy(omitNoneFields = value)

  private def copy(
      fieldNaming: FieldNaming = fieldNaming,
      discriminatorNaming: FieldNaming = discriminatorNaming,
      encodeEmptyCasesAsString: Boolean = encodeEmptyCasesAsString,
      omitNoneFields: Boolean = omitNoneFields,
  ): WireCodecConfig =
    new WireCodecConfig(
      fieldNaming = fieldNaming,
      discriminatorNaming = discriminatorNaming,
      encodeEmptyCasesAsString = encodeEmptyCasesAsString,
      omitNoneFields = omitNoneFields
    )

object WireCodecConfig:
  val Default: WireCodecConfig = new WireCodecConfig(
    fieldNaming = FieldNaming.identity,
    discriminatorNaming = FieldNaming.identity,
    encodeEmptyCasesAsString = false,
    omitNoneFields = true,
  )

  val SnakeCase: WireCodecConfig = Default.withFieldNaming(FieldNaming.snakeCase)

  given default: WireCodecConfig = Default
