package mongo4s.results

import org.bson.BsonValue

opaque type InsertManyResult = List[BsonValue]

object InsertManyResult:
  def apply(insertedIds: List[BsonValue]): InsertManyResult = insertedIds

  extension (result: InsertManyResult) def insertedIds: List[BsonValue] = result
