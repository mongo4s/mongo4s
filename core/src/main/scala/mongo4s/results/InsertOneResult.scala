package mongo4s.results

import org.bson.BsonValue

opaque type InsertOneResult = Option[BsonValue]

object InsertOneResult:
  def apply(insertedId: Option[BsonValue]): InsertOneResult = insertedId

  extension (result: InsertOneResult) def insertedId: Option[BsonValue] = result
