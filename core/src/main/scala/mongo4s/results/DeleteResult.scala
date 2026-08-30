package mongo4s.results

opaque type DeleteResult = Long

object DeleteResult:
  def apply(deletedCount: Long): DeleteResult = deletedCount

  val none: DeleteResult = 0L

  extension (result: DeleteResult)
    inline def deletedCount: Long  = result
    inline def deletedAny: Boolean = result > 0
