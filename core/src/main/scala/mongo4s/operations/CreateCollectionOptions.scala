package mongo4s.operations

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument

import mongo4s.FieldPath
import com.mongodb.client.model.{
  Collation,
  ValidationLevel,
  ValidationAction,
  ValidationOptions,
  TimeSeriesGranularity,
  TimeSeriesOptions as DriverTimeSeriesOptions,
  ClusteredIndexOptions as DriverClusteredIndexOptions,
  CreateCollectionOptions as DriverCreateCollectionOptions,
}

final class TimeSeries private (
    val timeField: String,
    val metaField: Option[String],
    val granularity: Option[TimeSeriesGranularity],
):
  def withMetaField(value: String): TimeSeries = copy(metaField = Some(value))

  def withGranularity(value: TimeSeriesGranularity): TimeSeries = copy(granularity = Some(value))

  def toDriver: DriverTimeSeriesOptions =
    val options = DriverTimeSeriesOptions(timeField)

    metaField.foreach(options.metaField)
    granularity.foreach(options.granularity)

    options

  private def copy(
      metaField: Option[String] = metaField,
      granularity: Option[TimeSeriesGranularity] = granularity,
  ): TimeSeries =
    new TimeSeries(timeField, metaField, granularity)

object TimeSeries:
  def on(timeField: String): TimeSeries = new TimeSeries(timeField, None, None)

final class ClusteredIndex private (val key: BsonDocument, val name: Option[String]):
  def named(value: String): ClusteredIndex = new ClusteredIndex(key, Some(value))

  def toDriver: DriverClusteredIndexOptions =
    val options = DriverClusteredIndexOptions(key, true)

    name.foreach(options.name)

    options
  end toDriver

object ClusteredIndex:
  val onId: ClusteredIndex = new ClusteredIndex(BsonDocument(FieldPath.IdName, org.bson.BsonInt32(1)), None)

final class CreateCollectionOptions private (
    val capped: Option[Long],
    val maxDocuments: Option[Long],
    val validator: Option[BsonDocument],
    val validationLevel: Option[ValidationLevel],
    val validationAction: Option[ValidationAction],
    val timeSeries: Option[TimeSeries],
    val clusteredIndex: Option[ClusteredIndex],
    val expireAfter: Option[FiniteDuration],
    val collation: Option[Collation],
    val storageEngine: Option[BsonDocument],
):
  def withCapped(sizeInBytes: Long): CreateCollectionOptions = copy(capped = Some(sizeInBytes))

  def withMaxDocuments(value: Long): CreateCollectionOptions = copy(maxDocuments = Some(value))

  def withValidator(value: BsonDocument): CreateCollectionOptions = copy(validator = Some(value))

  def withValidationLevel(value: ValidationLevel): CreateCollectionOptions = copy(validationLevel = Some(value))

  def withValidationAction(value: ValidationAction): CreateCollectionOptions = copy(validationAction = Some(value))

  def withTimeSeries(value: TimeSeries): CreateCollectionOptions = copy(timeSeries = Some(value))

  def withClusteredIndex(value: ClusteredIndex): CreateCollectionOptions = copy(clusteredIndex = Some(value))

  def expiringAfter(duration: FiniteDuration): CreateCollectionOptions =
    require(
      duration.toSeconds > 0,
      s"expireAfterSeconds must be at least one second — MongoDB stores it as a whole number, and $duration would truncate to 0",
    )
    copy(expireAfter = Some(duration))
  end expiringAfter

  def withCollation(value: Collation): CreateCollectionOptions = copy(collation = Some(value))

  def withStorageEngine(value: BsonDocument): CreateCollectionOptions = copy(storageEngine = Some(value))

  def toDriver: DriverCreateCollectionOptions =
    require(
      validator.isDefined || (validationLevel.isEmpty && validationAction.isEmpty),
      "a validation level or action needs a validator to apply to — the server ignores them on their own",
    )
    require(
      maxDocuments.isEmpty || capped.isDefined,
      "maxDocuments only applies to a capped collection — add withCapped(sizeInBytes)",
    )

    val options = DriverCreateCollectionOptions()

    capped.foreach { size =>
      options.capped(true)
      options.sizeInBytes(size)
    }
    maxDocuments.foreach(options.maxDocuments)

    validator.foreach { document =>
      val validation = ValidationOptions().validator(document)

      validationLevel.foreach(validation.validationLevel)
      validationAction.foreach(validation.validationAction)

      options.validationOptions(validation)
    }

    timeSeries.foreach(value => options.timeSeriesOptions(value.toDriver))
    clusteredIndex.foreach(value => options.clusteredIndexOptions(value.toDriver))
    expireAfter.foreach(duration => options.expireAfter(duration.toSeconds, TimeUnit.SECONDS))
    collation.foreach(options.collation)
    storageEngine.foreach(options.storageEngineOptions)

    options
  end toDriver

  private def copy(
      capped: Option[Long] = capped,
      maxDocuments: Option[Long] = maxDocuments,
      validator: Option[BsonDocument] = validator,
      validationLevel: Option[ValidationLevel] = validationLevel,
      validationAction: Option[ValidationAction] = validationAction,
      timeSeries: Option[TimeSeries] = timeSeries,
      clusteredIndex: Option[ClusteredIndex] = clusteredIndex,
      expireAfter: Option[FiniteDuration] = expireAfter,
      collation: Option[Collation] = collation,
      storageEngine: Option[BsonDocument] = storageEngine,
  ): CreateCollectionOptions =
    new CreateCollectionOptions(
      capped = capped,
      maxDocuments = maxDocuments,
      validator = validator,
      validationLevel = validationLevel,
      validationAction = validationAction,
      timeSeries = timeSeries,
      clusteredIndex = clusteredIndex,
      expireAfter = expireAfter,
      collation = collation,
      storageEngine = storageEngine,
    )

object CreateCollectionOptions:
  val default: CreateCollectionOptions =
    new CreateCollectionOptions(
      capped = None,
      maxDocuments = None,
      validator = None,
      validationLevel = None,
      validationAction = None,
      timeSeries = None,
      clusteredIndex = None,
      expireAfter = None,
      collation = None,
      storageEngine = None,
    )

  def capped(sizeInBytes: Long): CreateCollectionOptions = default.withCapped(sizeInBytes)

  def timeSeries(timeField: String): CreateCollectionOptions = default.withTimeSeries(TimeSeries.on(timeField))
