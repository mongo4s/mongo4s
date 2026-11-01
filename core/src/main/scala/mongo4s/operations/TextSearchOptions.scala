package mongo4s.operations

final class TextSearchOptions private (
    val language: Option[String],
    val caseSensitive: Option[Boolean],
    val diacriticSensitive: Option[Boolean],
):
  def withLanguage(value: String): TextSearchOptions = copy(language = Some(value))

  def withCaseSensitive(value: Boolean): TextSearchOptions = copy(caseSensitive = Some(value))

  def withDiacriticSensitive(value: Boolean): TextSearchOptions = copy(diacriticSensitive = Some(value))

  private def copy(
      language: Option[String] = language,
      caseSensitive: Option[Boolean] = caseSensitive,
      diacriticSensitive: Option[Boolean] = diacriticSensitive,
  ): TextSearchOptions =
    new TextSearchOptions(
      language = language,
      caseSensitive = caseSensitive,
      diacriticSensitive = diacriticSensitive,
    )

object TextSearchOptions:
  val default: TextSearchOptions =
    new TextSearchOptions(
      language = None,
      caseSensitive = None,
      diacriticSensitive = None,
    )
