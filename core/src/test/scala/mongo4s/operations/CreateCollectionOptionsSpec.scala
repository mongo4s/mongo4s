package mongo4s.operations

import java.util.concurrent.TimeUnit

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32}
import com.mongodb.client.model.{TimeSeriesGranularity, ValidationAction, ValidationLevel}

import scala.concurrent.duration.given

final class CreateCollectionOptionsSpec extends AnyWordSpec, Matchers:

  private val schema = BsonDocument("$jsonSchema", BsonDocument("required", BsonDocument()))

  "CreateCollectionOptions" should {

    "ask for nothing by default" in {
      val driver = CreateCollectionOptions.default.toDriver

      driver.isCapped shouldBe false
      driver.getValidationOptions.getValidator shouldBe null
      driver.getTimeSeriesOptions shouldBe null
      driver.getClusteredIndexOptions shouldBe null
    }

    "carry a capped size, and the document cap alongside it" in {
      val driver = CreateCollectionOptions.capped(4096).withMaxDocuments(100).toDriver

      driver.isCapped shouldBe true
      driver.getSizeInBytes shouldBe 4096L
      driver.getMaxDocuments shouldBe 100L
    }

    "refuse a document cap on a collection that is not capped" in {
      // The server ignores maxDocuments without capped, which reads as a cap that silently does not apply.
      an[IllegalArgumentException] should be thrownBy CreateCollectionOptions.default.withMaxDocuments(100).toDriver
    }

    "carry a validator with its level and action" in {
      val driver = CreateCollectionOptions.default
        .withValidator(schema)
        .withValidationLevel(ValidationLevel.MODERATE)
        .withValidationAction(ValidationAction.WARN)
        .toDriver

      driver.getValidationOptions.getValidator shouldBe schema
      driver.getValidationOptions.getValidationLevel shouldBe ValidationLevel.MODERATE
      driver.getValidationOptions.getValidationAction shouldBe ValidationAction.WARN
    }

    "refuse a validation level with no validator to apply it to" in {
      an[IllegalArgumentException] should be thrownBy
        CreateCollectionOptions.default.withValidationLevel(ValidationLevel.MODERATE).toDriver
    }

    "describe a time series by its time, meta and granularity" in {
      val driver = CreateCollectionOptions
        .timeSeries("recordedAt")
        .withTimeSeries(TimeSeries.on("recordedAt").withMetaField("sensor").withGranularity(TimeSeriesGranularity.MINUTES))
        .toDriver

      driver.getTimeSeriesOptions.getTimeField shouldBe "recordedAt"
      driver.getTimeSeriesOptions.getMetaField shouldBe "sensor"
      driver.getTimeSeriesOptions.getGranularity shouldBe TimeSeriesGranularity.MINUTES
    }

    "cluster on _id, uniquely, under the name it is given" in {
      val driver = CreateCollectionOptions.default.withClusteredIndex(ClusteredIndex.onId.named("primary")).toDriver

      driver.getClusteredIndexOptions.getKey shouldBe BsonDocument("_id", BsonInt32(1))
      driver.getClusteredIndexOptions.isUnique shouldBe true
      driver.getClusteredIndexOptions.getName shouldBe "primary"
    }

    "store a whole number of seconds for the expiry" in {
      CreateCollectionOptions.default.expiringAfter(90.seconds).toDriver.getExpireAfter(TimeUnit.SECONDS) shouldBe 90L
    }

    "refuse a sub-second expiry rather than truncate it to zero" in {
      an[IllegalArgumentException] should be thrownBy CreateCollectionOptions.default.expiringAfter(500.millis)
    }
  }
