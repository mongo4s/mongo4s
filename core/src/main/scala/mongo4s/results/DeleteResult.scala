package mongo4s.results

import com.mongodb.client.result.DeleteResult as DriverDeleteResult

opaque type DeleteResult = Long

object DeleteResult:
  def apply(deletedCount: Long): DeleteResult = deletedCount

  val none: DeleteResult = 0L

  def fromDriver(result: DriverDeleteResult): DeleteResult =
    if result.wasAcknowledged
    then result.getDeletedCount
    else none

  extension (result: DeleteResult)
    inline def deletedCount: Long  = result
    inline def deletedAny: Boolean = result > 0
