package mongo4s.results

import org.bson.BsonValue
import com.mongodb.client.result.InsertOneResult as DriverInsertOneResult

opaque type InsertOneResult = Option[BsonValue]

object InsertOneResult:
  def apply(insertedId: Option[BsonValue]): InsertOneResult = insertedId

  def fromDriver(result: DriverInsertOneResult): InsertOneResult =
    if result.wasAcknowledged
    then Option(result.getInsertedId)
    else None

  extension (result: InsertOneResult) inline def insertedId: Option[BsonValue] = result
