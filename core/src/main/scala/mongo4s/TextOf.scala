package mongo4s

@annotation.implicitNotFound("${C} does not hold text, so $$regex does not apply to it")
trait TextOf[C]

object TextOf:
  private val instance: TextOf[Any] = new TextOf[Any] {}

  private def of[C]: TextOf[C] = instance.asInstanceOf[TextOf[C]]

  given string: TextOf[String]                             = of
  given optional: TextOf[Option[String]]                   = of
  given elements: [C] => ElementOf[C, String] => TextOf[C] = of
