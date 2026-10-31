package mongo4s.bson.direct

import mongo4s.bson.FieldNaming

final class WireCodecConfig private (
    val fieldNaming: FieldNaming,
    val discriminatorNaming: FieldNaming,
    val encodeEmptyCasesAsString: Boolean,
    val omitNoneFields: Boolean,
    val decodeMissingAsDefault: Boolean,
    val discriminatorField: String,
    val wrappedCases: Boolean,
    val fallbackCase: Option[String],
):
  require(discriminatorField.nonEmpty, "the discriminator field needs a name")

  def withFieldNaming(value: FieldNaming): WireCodecConfig =
    copy(fieldNaming = value)

  def withDiscriminatorNaming(value: FieldNaming): WireCodecConfig =
    copy(discriminatorNaming = value)

  def withEncodeEmptyCasesAsString(value: Boolean): WireCodecConfig =
    copy(encodeEmptyCasesAsString = value)

  def withOmitNoneFields(value: Boolean): WireCodecConfig =
    copy(omitNoneFields = value)

  def withDecodeMissingAsDefault(value: Boolean): WireCodecConfig =
    copy(decodeMissingAsDefault = value)

  def withDiscriminatorField(name: String): WireCodecConfig =
    copy(discriminatorField = name)

  def withWrappedCases(value: Boolean): WireCodecConfig =
    copy(wrappedCases = value)

  def withFallbackCase(discriminator: String): WireCodecConfig =
    copy(fallbackCase = Some(discriminator))

  private def copy(
      fieldNaming: FieldNaming = fieldNaming,
      discriminatorNaming: FieldNaming = discriminatorNaming,
      encodeEmptyCasesAsString: Boolean = encodeEmptyCasesAsString,
      omitNoneFields: Boolean = omitNoneFields,
      decodeMissingAsDefault: Boolean = decodeMissingAsDefault,
      discriminatorField: String = discriminatorField,
      wrappedCases: Boolean = wrappedCases,
      fallbackCase: Option[String] = fallbackCase,
  ): WireCodecConfig =
    new WireCodecConfig(
      fieldNaming = fieldNaming,
      discriminatorNaming = discriminatorNaming,
      encodeEmptyCasesAsString = encodeEmptyCasesAsString,
      omitNoneFields = omitNoneFields,
      decodeMissingAsDefault = decodeMissingAsDefault,
      discriminatorField = discriminatorField,
      wrappedCases = wrappedCases,
      fallbackCase = fallbackCase,
    )

object WireCodecConfig:
  val Default: WireCodecConfig = new WireCodecConfig(
    fieldNaming = FieldNaming.identity,
    discriminatorNaming = FieldNaming.identity,
    encodeEmptyCasesAsString = false,
    omitNoneFields = true,
    decodeMissingAsDefault = false,
    discriminatorField = WireDiscriminator.Field,
    wrappedCases = false,
    fallbackCase = None,
  )

  val SnakeCase: WireCodecConfig = Default.withFieldNaming(FieldNaming.snakeCase)

  given default: WireCodecConfig = Default
