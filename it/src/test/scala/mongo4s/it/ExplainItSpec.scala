package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}
import com.mongodb.ExplainVerbosity

import mongo4s.cats.CatsStream
import mongo4s.operations.{Index, Stage}
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class ExplainItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val email = Field.stored[BsonDocument, String]("email")
  private val city  = Field.stored[BsonDocument, String]("city")

  private def person(address: String, town: String): BsonDocument =
    BsonDocument("email", BsonString(address)).append("city", BsonString(town))

  private def seeded(name: String): IO[MongoCollection[IO, S, BsonDocument]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("explain_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.createIndex(Index.ascending(email))
      _          <- collection.insertMany(List(person("a@b.c", "berlin"), person("d@e.f", "prague")))
    yield collection

  "explain on a find" should {

    "report the index a filter on an indexed field uses" in {
      val program =
        for
          collection <- seeded("indexed")
          plan       <- collection.find(email.equalTo("a@b.c")).explain()
        yield plan.toJson

      program.timeout(30.seconds).asserting { json =>
        json should include("IXSCAN")
        json should include("email_1")
      }
    }

    "report a collection scan when the filter has no index to use" in {
      val program =
        for
          collection <- seeded("unindexed")
          plan       <- collection.find(city.equalTo("berlin")).explain()
        yield plan.toJson

      program.timeout(30.seconds).asserting { json =>
        json should include("COLLSCAN")
        json should not include "IXSCAN"
      }
    }

    "carry the query the builder produced, not an empty one" in {
      val program =
        for
          collection <- seeded("filter_shape")
          plan       <- collection.find(email.equalTo("a@b.c")).explain()
        yield plan.toJson

      program.timeout(30.seconds).asserting(_ should include("a@b.c"))
    }

    "add execution statistics when asked for them" in {
      val program =
        for
          collection <- seeded("stats")
          planner    <- collection.find(email.equalTo("a@b.c")).explain()
          stats      <- collection.find(email.equalTo("a@b.c")).explain(ExplainVerbosity.EXECUTION_STATS)
        yield (planner.containsKey("executionStats"), stats.containsKey("executionStats"))

      program.timeout(30.seconds).asserting(_ shouldBe (false, true))
    }
  }

  "explain on an aggregation" should {

    "describe the pipeline rather than running it" in {
      val program =
        for
          collection <- seeded("pipeline")
          plan       <- collection
                          .aggregate[BsonDocument](Seq(Stage.matching(email.equalTo("a@b.c")), Stage.limit(1)))
                          .explain()
        yield plan.toJson

      program.timeout(30.seconds).asserting { json =>
        json should include("a@b.c")
        json should include("IXSCAN")
      }
    }

    "explain the whole pipeline, not the one-document form first uses" in {
      val program =
        for
          collection <- seeded("unlimited")
          plan       <- collection.aggregate[BsonDocument](Seq(Stage.matching(email.equalTo("a@b.c")))).explain()
        yield plan.toJson

      program.timeout(30.seconds).asserting(_ should not include "\"$limit\": 1")
    }
  }
