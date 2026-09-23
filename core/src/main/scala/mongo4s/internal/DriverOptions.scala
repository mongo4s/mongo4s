package mongo4s.internal

import java.util.concurrent.TimeUnit

import com.mongodb.client.model.{IndexOptions, ReturnDocument}
import com.mongodb.client.model.{CountOptions as DriverCountOptions, DeleteOptions as DriverDeleteOptions}
import com.mongodb.client.model.{ReplaceOptions as DriverReplaceOptions, UpdateOptions as DriverUpdateOptions}
import com.mongodb.client.model.{FindOneAndDeleteOptions as DriverFindOneAndDeleteOptions, FindOneAndReplaceOptions as DriverFindOneAndReplaceOptions}
import com.mongodb.client.model.FindOneAndUpdateOptions as DriverFindOneAndUpdateOptions

import mongo4s.operations.*
import mongo4s.bson.FieldNaming

import scala.jdk.CollectionConverters.given

private[mongo4s] object DriverOptions:

  def update(options: UpdateOptions, naming: FieldNaming): DriverUpdateOptions =
    val driverOptions = DriverUpdateOptions().upsert(options.upsert)

    if options.arrayFilters.nonEmpty
    then driverOptions.arrayFilters(options.arrayFilters.map(_.toBson(naming)).asJava): Unit

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.comment.foreach(value => driverOptions.comment(value))
    options.bypassDocumentValidation.foreach(value => driverOptions.bypassDocumentValidation(value))

    driverOptions
  end update

  def replace(options: ReplaceOptions): DriverReplaceOptions =
    val driverOptions = DriverReplaceOptions().upsert(options.upsert)

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.comment.foreach(value => driverOptions.comment(value))
    options.bypassDocumentValidation.foreach(value => driverOptions.bypassDocumentValidation(value))

    driverOptions
  end replace

  def delete(options: DeleteOptions): DriverDeleteOptions =
    val driverOptions = DriverDeleteOptions()

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.comment.foreach(value => driverOptions.comment(value))

    driverOptions
  end delete

  def count(options: CountOptions): DriverCountOptions =
    val driverOptions = DriverCountOptions()

    options.collation.foreach(value => driverOptions.collation(value))
    options.hint.foreach(value => driverOptions.hint(value))
    options.limit.foreach(value => driverOptions.limit(value))
    options.skip.foreach(value => driverOptions.skip(value))
    options.maxTime.foreach(value => driverOptions.maxTime(value.toMillis, TimeUnit.MILLISECONDS))

    driverOptions
  end count

  def findOneAndUpdate[A](options: FindOneAndUpdateOptions[A], naming: FieldNaming): DriverFindOneAndUpdateOptions =
    val driverOptions = DriverFindOneAndUpdateOptions()
      .upsert(options.upsert)
      .returnDocument(if options.returnUpdated then ReturnDocument.AFTER else ReturnDocument.BEFORE)

    if !options.sort.isEmpty then driverOptions.sort(options.sort.toBson(naming)): Unit
    if !options.projection.isEmpty then driverOptions.projection(options.projection.toBson(naming)): Unit
    if options.arrayFilters.nonEmpty then driverOptions.arrayFilters(options.arrayFilters.map(_.toBson(naming)).asJava): Unit

    driverOptions
  end findOneAndUpdate

  def findOneAndReplace[A](options: FindOneAndReplaceOptions[A], naming: FieldNaming): DriverFindOneAndReplaceOptions =
    val driverOptions = DriverFindOneAndReplaceOptions()
      .upsert(options.upsert)
      .returnDocument(if options.returnUpdated then ReturnDocument.AFTER else ReturnDocument.BEFORE)

    if !options.sort.isEmpty then driverOptions.sort(options.sort.toBson(naming)): Unit
    if !options.projection.isEmpty then driverOptions.projection(options.projection.toBson(naming)): Unit

    driverOptions
  end findOneAndReplace

  def findOneAndDelete[A](options: FindOneAndDeleteOptions[A], naming: FieldNaming): DriverFindOneAndDeleteOptions =
    val driverOptions = DriverFindOneAndDeleteOptions()

    if !options.sort.isEmpty then driverOptions.sort(options.sort.toBson(naming)): Unit
    if !options.projection.isEmpty then driverOptions.projection(options.projection.toBson(naming)): Unit

    driverOptions
  end findOneAndDelete

  def index[A](index: Index[A], naming: FieldNaming): IndexOptions =
    val options = IndexOptions().unique(index.unique).sparse(index.sparse).hidden(index.hidden)

    index.name.foreach(options.name)
    index.expireAfter.foreach(duration => options.expireAfter(duration.toSeconds, TimeUnit.SECONDS))
    index.partialFilter.foreach(filter => options.partialFilterExpression(filter.toBson(naming)))
    index.collation.foreach(options.collation)
    index.wildcardProjection.foreach(options.wildcardProjection)

    options
  end index

end DriverOptions
