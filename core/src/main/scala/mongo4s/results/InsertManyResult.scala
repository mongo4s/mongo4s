package mongo4s.results

import java.util.Map as JMap

import org.bson.BsonValue

import scala.jdk.CollectionConverters.given

opaque type InsertManyResult = List[BsonValue]

object InsertManyResult:
  def apply(insertedIds: List[BsonValue]): InsertManyResult = insertedIds

  def fromDriver(insertedIds: JMap[Integer, BsonValue]): InsertManyResult =
    insertedIds.asScala.toList.sortBy(_._1.intValue).map(_._2)

  extension (result: InsertManyResult) inline def insertedIds: List[BsonValue] = result
