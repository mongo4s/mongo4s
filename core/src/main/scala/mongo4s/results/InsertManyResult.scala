package mongo4s.results

import org.bson.BsonValue
import com.mongodb.client.result.InsertManyResult as DriverInsertManyResult

import scala.jdk.CollectionConverters.given

opaque type InsertManyResult = List[BsonValue]

object InsertManyResult:
  def apply(insertedIds: List[BsonValue]): InsertManyResult = insertedIds

  def fromDriver(result: DriverInsertManyResult): InsertManyResult =
    if result.wasAcknowledged
    then result.getInsertedIds.asScala.toList.sortBy(_._1.intValue).map(_._2)
    else Nil

  extension (result: InsertManyResult) inline def insertedIds: List[BsonValue] = result
