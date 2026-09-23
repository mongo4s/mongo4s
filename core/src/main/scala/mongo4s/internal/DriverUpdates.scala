package mongo4s.internal

import org.bson.BsonDocument
import org.reactivestreams.Publisher
import com.mongodb.client.result.UpdateResult as DriverUpdateResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}
import com.mongodb.client.model.{UpdateManyModel, UpdateOneModel, UpdateOptions as DriverUpdateOptions}
import com.mongodb.client.model.FindOneAndUpdateOptions as DriverFindOneAndUpdateOptions

import mongo4s.operations.Update
import mongo4s.bson.FieldNaming

import scala.jdk.CollectionConverters.given

private[mongo4s] object DriverUpdates:

  def updateOne[T](
      collection: RSMongoCollection[T],
      session: Option[ClientSession],
      filter: BsonDocument,
      update: Update[?],
      naming: FieldNaming,
      options: DriverUpdateOptions,
  ): Publisher[DriverUpdateResult] =
    (session, update.toPipeline(naming)) match
      case (Some(s), Some(stages)) => collection.updateOne(s, filter, stages.asJava, options)
      case (None, Some(stages))    => collection.updateOne(filter, stages.asJava, options)
      case (Some(s), None)         => collection.updateOne(s, filter, update.toBson(naming), options)
      case (None, None)            => collection.updateOne(filter, update.toBson(naming), options)

  def updateMany[T](
      collection: RSMongoCollection[T],
      session: Option[ClientSession],
      filter: BsonDocument,
      update: Update[?],
      naming: FieldNaming,
      options: DriverUpdateOptions,
  ): Publisher[DriverUpdateResult] =
    (session, update.toPipeline(naming)) match
      case (Some(s), Some(stages)) => collection.updateMany(s, filter, stages.asJava, options)
      case (None, Some(stages))    => collection.updateMany(filter, stages.asJava, options)
      case (Some(s), None)         => collection.updateMany(s, filter, update.toBson(naming), options)
      case (None, None)            => collection.updateMany(filter, update.toBson(naming), options)

  def findOneAndUpdate[T](
      collection: RSMongoCollection[T],
      session: Option[ClientSession],
      filter: BsonDocument,
      update: Update[?],
      naming: FieldNaming,
      options: DriverFindOneAndUpdateOptions,
  ): Publisher[T] =
    (session, update.toPipeline(naming)) match
      case (Some(s), Some(stages)) => collection.findOneAndUpdate(s, filter, stages.asJava, options)
      case (None, Some(stages))    => collection.findOneAndUpdate(filter, stages.asJava, options)
      case (Some(s), None)         => collection.findOneAndUpdate(s, filter, update.toBson(naming), options)
      case (None, None)            => collection.findOneAndUpdate(filter, update.toBson(naming), options)

  def updateOneModel[T](filter: BsonDocument, update: Update[?], naming: FieldNaming, options: DriverUpdateOptions): UpdateOneModel[T] =
    update.toPipeline(naming) match
      case Some(stages) => UpdateOneModel(filter, stages.asJava, options)
      case None         => UpdateOneModel(filter, update.toBson(naming), options)

  def updateManyModel[T](filter: BsonDocument, update: Update[?], naming: FieldNaming, options: DriverUpdateOptions): UpdateManyModel[T] =
    update.toPipeline(naming) match
      case Some(stages) => UpdateManyModel(filter, stages.asJava, options)
      case None         => UpdateManyModel(filter, update.toBson(naming), options)

end DriverUpdates
