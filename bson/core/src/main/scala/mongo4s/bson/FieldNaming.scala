package mongo4s.bson

trait FieldNaming:
  def apply(field: String): String

object FieldNaming:
  val identity: FieldNaming = field => field

  val snakeCase: FieldNaming = field =>
    field
      .foldLeft(new StringBuilder) { (sb, ch) =>
        if ch.isUpper
        then
          if sb.nonEmpty
          then sb.append('_')
          sb.append(ch.toLower)
        else sb.append(ch)
      }
      .toString

  val kebabCase: FieldNaming = field =>
    field
      .foldLeft(new StringBuilder) { (sb, ch) =>
        if ch.isUpper
        then
          if sb.nonEmpty
          then sb.append('-')
          sb.append(ch.toLower)
        else sb.append(ch)
      }
      .toString

  def overrides(mapping: Map[String, String], fallback: FieldNaming = identity): FieldNaming =
    field => mapping.getOrElse(field, fallback(field))
