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
import mongo4s.operations.{Index, Sort, Stage}
import mongo4s.results.ExplainSummary
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class ExplainItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2")

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

  "ExplainSummary" should {

    "name the index a plan used" in {
      val program =
        for
          collection <- seeded("summary_indexed")
          summary    <- collection.find(email.equalTo("a@b.c")).explain().map(ExplainSummary.of)
        yield summary

      program.timeout(30.seconds).asserting { summary =>
        summary.indexes shouldBe List("email_1")
        summary.usedIndex shouldBe true
        summary.scannedCollection shouldBe false
      }
    }

    "report a collection scan, with no index to name" in {
      val program =
        for
          collection <- seeded("summary_collscan")
          summary    <- collection.find(city.equalTo("berlin")).explain().map(ExplainSummary.of)
        yield summary

      program.timeout(30.seconds).asserting { summary =>
        summary.indexes shouldBe empty
        summary.scannedCollection shouldBe true
      }
    }

    "tell an index-provided order apart from a blocking sort" in {
      val program =
        for
          collection <- seeded("summary_sort")
          byIndex    <- collection.find().sort(Sort.asc(email)).explain().map(ExplainSummary.of)
          inMemory   <- collection.find().sort(Sort.asc(city)).explain().map(ExplainSummary.of)
        yield (byIndex.sortedInMemory, inMemory.sortedInMemory)

      program.timeout(30.seconds).asserting(_ shouldBe (false, true))
    }

    "carry execution counts only when they were asked for" in {
      val program =
        for
          collection <- seeded("summary_stats")
          planner    <- collection.find(email.equalTo("a@b.c")).explain().map(ExplainSummary.of)
          measured   <- collection
                          .find(email.equalTo("a@b.c"))
                          .explain(ExplainVerbosity.EXECUTION_STATS)
                          .map(ExplainSummary.of)
        yield (planner.execution, measured.execution)

      program.timeout(30.seconds).asserting { (planner, measured) =>
        planner shouldBe None
        measured.map(_.returned) shouldBe Some(1L)
        measured.map(_.docsExamined) shouldBe Some(1L)
      }
    }

    "read the same summary out of both shapes an aggregation explain can take" in {
      val program =
        for
          collection <- seeded("summary_shapes")
          optimised  <- collection
                          .aggregate[BsonDocument](Seq(Stage.matching(email.equalTo("a@b.c"))))
                          .explain()
          staged     <- collection
                          .aggregate[BsonDocument](
                            Seq(Stage.facet("byCity" -> List(Stage.groupBy(city)("n" -> mongo4s.operations.Accumulator.count[BsonDocument]))))
                          )
                          .explain()
        yield (optimised, staged)

      program.timeout(30.seconds).asserting { (optimised, staged) =>
        optimised.containsKey("queryPlanner") shouldBe true
        optimised.containsKey("stages") shouldBe false
        staged.containsKey("stages") shouldBe true
        staged.containsKey("queryPlanner") shouldBe false

        ExplainSummary.of(optimised).indexes shouldBe List("email_1")
        ExplainSummary.of(staged).scannedCollection shouldBe true
      }
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
