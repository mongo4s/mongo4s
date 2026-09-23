package mongo4s

@annotation.implicitNotFound("distinct over a field of ${C} cannot answer with ${B}")
trait DistinctOf[C, B]

object DistinctOf extends LowPriorityDistinctOf:
  given elements: [C, A] => ElementOf[C, A] => DistinctOf[C, A] = of

trait LowPriorityDistinctOf:
  private val instance: DistinctOf[Any, Any] = new DistinctOf[Any, Any] {}

  protected def of[C, B]: DistinctOf[C, B] = instance.asInstanceOf[DistinctOf[C, B]]

  given value: [A] => DistinctOf[A, A] = of
