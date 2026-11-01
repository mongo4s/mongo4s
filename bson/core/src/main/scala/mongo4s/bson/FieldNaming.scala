package mongo4s.bson

trait FieldNaming:
  def apply(field: String): String

object FieldNaming:
  val identity: FieldNaming = field => field

  val snakeCase: FieldNaming = separated(_, '_')

  val kebabCase: FieldNaming = separated(_, '-')

  private def separated(field: String, separator: Char): String =
    var i = 0
    while i < field.length && !field.charAt(i).isUpper do i += 1

    if i == field.length
    then field
    else
      val renamed = new java.lang.StringBuilder(field.length + 4)
      renamed.append(field, 0, i): Unit

      while i < field.length
      do
        val ch = field.charAt(i)
        if ch.isUpper
        then
          if renamed.length > 0 then renamed.append(separator): Unit
          renamed.append(ch.toLower): Unit
        else renamed.append(ch): Unit
        i += 1
      end while

      renamed.toString
  end separated

  def overrides(mapping: Map[String, String], fallback: FieldNaming = identity): FieldNaming =
    field => mapping.getOrElse(field, fallback(field))
