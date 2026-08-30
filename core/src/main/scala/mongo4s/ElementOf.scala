package mongo4s

@annotation.implicitNotFound("${C} is not a collection of ${A}, so this array operator does not apply to it")
trait ElementOf[C, A]

object ElementOf:
  private val instance: ElementOf[Any, Any] = new ElementOf[Any, Any] {}

  private def of[C, A]: ElementOf[C, A] = instance.asInstanceOf[ElementOf[C, A]]

  given list[A]: ElementOf[List[A], A]         = of
  given seq[A]: ElementOf[Seq[A], A]           = of
  given vector[A]: ElementOf[Vector[A], A]     = of
  given set[A]: ElementOf[Set[A], A]           = of
  given iterable[A]: ElementOf[Iterable[A], A] = of
  given array[A]: ElementOf[Array[A], A]       = of
