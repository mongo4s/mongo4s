package mongo4s.results

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonInt64, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

final class ExplainSummarySpec extends AnyWordSpec, Matchers:

  private def stage(name: String, fields: (String, BsonValue)*): BsonDocument =
    fields.foldLeft(BsonDocument("stage", BsonString(name)))((acc, entry) => acc.append(entry._1, entry._2))

  private def array(values: BsonValue*): BsonArray = BsonArray(values.toList.asJava)

  "ExplainSummary.of" should {

    "find a plan however deeply it is nested" in {
      val explain = BsonDocument(
        "queryPlanner",
        BsonDocument("winningPlan", stage("FETCH", "inputStage" -> stage("IXSCAN", "indexName" -> BsonString("email_1")))),
      )

      val summary = ExplainSummary.of(explain)

      summary.indexes shouldBe List("email_1")
      summary.stages shouldBe List("FETCH", "IXSCAN")
    }

    "walk into arrays, which is how a staged pipeline and a sharded plan carry theirs" in {
      val explain = BsonDocument(
        "stages",
        array(
          BsonDocument("$cursor", BsonDocument("queryPlanner", BsonDocument("winningPlan", stage("COLLSCAN")))),
          BsonDocument("$group", BsonDocument()),
        ),
      )

      ExplainSummary.of(explain).scannedCollection shouldBe true
    }

    "report each index and stage once, though execution stats repeat the whole plan" in {
      val plan    = stage("FETCH", "inputStage" -> stage("IXSCAN", "indexName" -> BsonString("email_1")))
      val explain = BsonDocument("queryPlanner", BsonDocument("winningPlan", plan))
        .append("executionStats", BsonDocument("nReturned", BsonInt32(1)).append("executionStages", plan))

      val summary = ExplainSummary.of(explain)

      summary.indexes shouldBe List("email_1")
      summary.stages shouldBe List("FETCH", "IXSCAN")
    }

    "read execution counts whatever width the server sent them in" in {
      val explain = BsonDocument(
        "executionStats",
        BsonDocument("nReturned", BsonInt32(3))
          .append("totalKeysExamined", BsonInt64(4L))
          .append("totalDocsExamined", BsonInt32(5))
          .append("executionTimeMillis", BsonInt32(6)),
      )

      ExplainSummary.of(explain).execution shouldBe Some(ExecutionSummary(3, 4, 5, 6))
    }

    "leave execution empty when the plan was never run" in {
      ExplainSummary.of(BsonDocument("queryPlanner", BsonDocument("winningPlan", stage("COLLSCAN")))).execution shouldBe None
    }

    "say nothing rather than guess when the document is not an explain at all" in {
      val summary = ExplainSummary.of(BsonDocument("ok", BsonInt32(1)))

      summary.indexes shouldBe empty
      summary.stages shouldBe empty
      summary.execution shouldBe None
      summary.usedIndex shouldBe false
      summary.scannedCollection shouldBe false
      summary.sortedInMemory shouldBe false
    }

    "separate a blocking sort from an order an index already provided" in {
      val blocking = BsonDocument("winningPlan", stage("SORT", "inputStage" -> stage("COLLSCAN")))
      val provided = BsonDocument("winningPlan", stage("FETCH", "inputStage" -> stage("IXSCAN", "indexName" -> BsonString("email_1"))))

      ExplainSummary.of(blocking).sortedInMemory shouldBe true
      ExplainSummary.of(provided).sortedInMemory shouldBe false
    }
  }
