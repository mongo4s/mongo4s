package mongo4s

@annotation.implicitNotFound("${C} holds no ${A}, so a path cannot continue into the fields of ${A} through it")
trait DocumentOf[C, A]

object DocumentOf:
  private val instance: DocumentOf[Any, Any] = new DocumentOf[Any, Any] {}

  private def of[C, A]: DocumentOf[C, A] = instance.asInstanceOf[DocumentOf[C, A]]

  given exact: [A] => DocumentOf[A, A]                          = of
  given optional: [A] => DocumentOf[Option[A], A]               = of
  given elements: [C, A] => ElementOf[C, A] => DocumentOf[C, A] = of
