package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}
import com.mongodb.client.model.{TimeSeriesGranularity, ValidationAction, ValidationLevel}

import mongo4s.cats.CatsStream
import mongo4s.operations.{ClusteredIndex, CreateCollectionOptions, TimeSeries}
import mongo4s.{MongoClient, MongoDatabase}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

final class CreateCollectionItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def database(name: String): IO[MongoDatabase[IO, S]] =
    MongoClient.fromConnectionString[IO, S](container.getConnectionString).flatMap(_.getDatabase(name))

  /** What the server itself says it created, rather than what we asked for. */
  private def optionsOf(db: MongoDatabase[IO, S], name: String): IO[BsonDocument] =
    db.listCollections.compile.toList
      .map(_.find(_.getString("name").getValue == name).getOrElse(fail(s"$name was not created")))
      .map(_.getDocument("options"))

  "createCollection" should {

    "cap a collection by size and by document count" in {
      val program =
        for
          db      <- database("create_capped")
          _       <- db.createCollection("events", CreateCollectionOptions.capped(8192).withMaxDocuments(50))
          options <- optionsOf(db, "events")
        yield options

      program.timeout(30.seconds).asserting { options =>
        options.getBoolean("capped").getValue shouldBe true
        options.getNumber("size").longValue shouldBe 8192L
        options.getNumber("max").longValue shouldBe 50L
      }
    }

    "attach a validator with its level and action" in {
      val schema = BsonDocument(
        "$jsonSchema",
        BsonDocument("bsonType", BsonString("object")),
      )

      val program =
        for
          db      <- database("create_validated")
          _       <- db.createCollection(
                       "people",
                       CreateCollectionOptions.default
                         .withValidator(schema)
                         .withValidationLevel(ValidationLevel.MODERATE)
                         .withValidationAction(ValidationAction.WARN),
                     )
          options <- optionsOf(db, "people")
        yield options

      program.timeout(30.seconds).asserting { options =>
        options.getDocument("validator") shouldBe schema
        options.getString("validationLevel").getValue shouldBe "moderate"
        options.getString("validationAction").getValue shouldBe "warn"
      }
    }

    "create a time-series collection with its meta field and granularity" in {
      val program =
        for
          db      <- database("create_timeseries")
          _       <- db.createCollection(
                       "readings",
                       CreateCollectionOptions.default
                         .withTimeSeries(TimeSeries.on("recordedAt").withMetaField("sensor").withGranularity(TimeSeriesGranularity.MINUTES))
                         .expiringAfter(3600.seconds),
                     )
          options <- optionsOf(db, "readings")
        yield options

      program.timeout(30.seconds).asserting { options =>
        val timeseries = options.getDocument("timeseries")

        timeseries.getString("timeField").getValue shouldBe "recordedAt"
        timeseries.getString("metaField").getValue shouldBe "sensor"
        timeseries.getString("granularity").getValue shouldBe "minutes"
        options.getNumber("expireAfterSeconds").longValue shouldBe 3600L
      }
    }

    "create a clustered collection" in {
      val program =
        for
          db      <- database("create_clustered")
          _       <- db.createCollection("orders", CreateCollectionOptions.default.withClusteredIndex(ClusteredIndex.onId.named("primary")))
          options <- optionsOf(db, "orders")
        yield options

      program.timeout(30.seconds).asserting { options =>
        val clustered = options.getDocument("clusteredIndex")

        clustered.getString("name").getValue shouldBe "primary"
        clustered.getBoolean("unique").getValue shouldBe true
      }
    }
  }
