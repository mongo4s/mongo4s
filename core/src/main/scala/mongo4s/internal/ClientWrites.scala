package mongo4s.internal

import com.mongodb.MongoNamespace
import com.mongodb.client.model.bulk.*

import mongo4s.bson.{BsonDocumentCodec, FieldNaming}
import mongo4s.operations.{BulkWriteOptions, DeleteOptions, ReplaceOptions, UpdateOptions, WriteCommand}

import scala.jdk.CollectionConverters.given

private[mongo4s] object ClientWrites:

  private val PerBulk = "a client bulk write takes a comment, let variables and bypassDocumentValidation once, on its BulkWriteOptions"

  def model[A](namespace: MongoNamespace, command: WriteCommand[A], codec: BsonDocumentCodec[A], naming: FieldNaming): ClientNamespacedWriteModel =
    command match
      case WriteCommand.InsertOne(document) =>
        ClientNamespacedWriteModel.insertOne(namespace, codec.encodeDocument(document))

      case WriteCommand.ReplaceOne(filter, replacement, options) =>
        ClientNamespacedWriteModel.replaceOne(namespace, filter.toBson(naming), codec.encodeDocument(replacement), replace(options, naming))

      case WriteCommand.UpdateOne(filter, update, options) =>
        val driverOptions = updateOne(options, naming)
        update.toPipeline(naming) match
          case Some(stages) => ClientNamespacedWriteModel.updateOne(namespace, filter.toBson(naming), stages.asJava, driverOptions)
          case None         => ClientNamespacedWriteModel.updateOne(namespace, filter.toBson(naming), update.toBson(naming), driverOptions)

      case WriteCommand.UpdateMany(filter, update, options) =>
        val driverOptions = updateMany(options, naming)
        update.toPipeline(naming) match
          case Some(stages) => ClientNamespacedWriteModel.updateMany(namespace, filter.toBson(naming), stages.asJava, driverOptions)
          case None         => ClientNamespacedWriteModel.updateMany(namespace, filter.toBson(naming), update.toBson(naming), driverOptions)

      case WriteCommand.DeleteOne(filter, options) =>
        val driverOptions = ClientDeleteOneOptions.clientDeleteOneOptions()
        perCommand(options)
        options.collation.foreach(value => driverOptions.collation(value))
        options.hint.foreach(value => driverOptions.hint(value))
        ClientNamespacedWriteModel.deleteOne(namespace, filter.toBson(naming), driverOptions)

      case WriteCommand.DeleteMany(filter, options) =>
        val driverOptions = ClientDeleteManyOptions.clientDeleteManyOptions()
        perCommand(options)
        options.collation.foreach(value => driverOptions.collation(value))
        options.hint.foreach(value => driverOptions.hint(value))
        ClientNamespacedWriteModel.deleteMany(namespace, filter.toBson(naming), driverOptions)
  end model

  def options(options: BulkWriteOptions): ClientBulkWriteOptions =
    val driverOptions = ClientBulkWriteOptions.clientBulkWriteOptions().ordered(options.ordered)

    options.comment.foreach(value => driverOptions.comment(org.bson.BsonString(value)))
    options.bypassDocumentValidation.foreach(value => driverOptions.bypassDocumentValidation(value))
    options.let.foreach(value => driverOptions.let(value))

    driverOptions
  end options

  private def replace(options: ReplaceOptions, naming: FieldNaming): ClientReplaceOneOptions =
    require(options.comment.isEmpty && options.let.isEmpty && options.bypassDocumentValidation.isEmpty, PerBulk)

    val driverOptions = ClientReplaceOneOptions.clientReplaceOneOptions().upsert(options.upsert)

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.sort.foreach(value => driverOptions.sort(value.toBson(naming)))

    driverOptions
  end replace

  private def updateOne(options: UpdateOptions, naming: FieldNaming): ClientUpdateOneOptions =
    perCommand(options)

    val driverOptions = ClientUpdateOneOptions.clientUpdateOneOptions().upsert(options.upsert)

    if options.arrayFilters.nonEmpty
    then driverOptions.arrayFilters(options.arrayFilters.map(_.toBson(naming)).asJava): Unit

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.sort.foreach(value => driverOptions.sort(value.toBson(naming)))

    driverOptions
  end updateOne

  private def updateMany(options: UpdateOptions, naming: FieldNaming): ClientUpdateManyOptions =
    perCommand(options)
    require(options.sort.isEmpty, DriverOptions.SortOnMany)

    val driverOptions = ClientUpdateManyOptions.clientUpdateManyOptions().upsert(options.upsert)

    if options.arrayFilters.nonEmpty
    then driverOptions.arrayFilters(options.arrayFilters.map(_.toBson(naming)).asJava): Unit

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))

    driverOptions
  end updateMany

  private def perCommand(options: UpdateOptions): Unit =
    require(options.comment.isEmpty && options.let.isEmpty && options.bypassDocumentValidation.isEmpty, PerBulk)

  private def perCommand(options: DeleteOptions): Unit =
    require(options.comment.isEmpty && options.let.isEmpty, PerBulk)

end ClientWrites
