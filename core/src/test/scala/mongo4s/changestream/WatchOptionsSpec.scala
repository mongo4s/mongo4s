package mongo4s.changestream

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonString, BsonTimestamp}
import com.mongodb.client.model.changestream.{FullDocument, FullDocumentBeforeChange}

import mongo4s.operations.Stage

import scala.concurrent.duration.given

final class WatchOptionsSpec extends AnyWordSpec, Matchers:

  private def token(value: String): BsonDocument = BsonDocument("_data", BsonString(value))

  "the default" should {
    "look up the current document rather than the server default" in {
      WatchOptions.default[String].fullDocument shouldBe FullDocument.UPDATE_LOOKUP
    }

    "start from now, with no pipeline" in {
      val options = WatchOptions.default[String]

      options.pipeline shouldBe empty
      options.resumeAfter shouldBe None
      options.startAfter shouldBe None
      options.startAtOperationTime shouldBe None
    }
  }

  "resume points" should {
    "be mutually exclusive: resumingAfter clears startAfter and the operation time" in {
      val options = WatchOptions.default[String].startingAfter(token("a")).startingAt(BsonTimestamp(1, 1)).resumingAfter(token("b"))

      options.resumeAfter shouldBe Some(token("b"))
      options.startAfter shouldBe None
      options.startAtOperationTime shouldBe None
    }

    "be mutually exclusive: startingAfter clears resumeAfter and the operation time" in {
      val options = WatchOptions.default[String].resumingAfter(token("a")).startingAt(BsonTimestamp(1, 1)).startingAfter(token("b"))

      options.startAfter shouldBe Some(token("b"))
      options.resumeAfter shouldBe None
      options.startAtOperationTime shouldBe None
    }

    "be mutually exclusive: startingAt clears both tokens" in {
      val options = WatchOptions.default[String].resumingAfter(token("a")).startingAt(BsonTimestamp(1, 1))

      options.startAtOperationTime shouldBe Some(BsonTimestamp(1, 1))
      options.resumeAfter shouldBe None
      options.startAfter shouldBe None
    }

    "expose resumeAfter as a one-step constructor" in {
      WatchOptions.resumeAfter[String](token("a")).resumeAfter shouldBe Some(token("a"))
    }
  }

  "the builders" should {
    "carry every option" in {
      val stages  = Seq(Stage.raw[String](BsonDocument("$match", BsonDocument("operationType", BsonString("insert")))))
      val options = WatchOptions
        .default[String]
        .withPipeline(stages)
        .withFullDocument(FullDocument.DEFAULT)
        .withFullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
        .startingAt(BsonTimestamp(1, 1))
        .withMaxAwaitTime(2.seconds)
        .withBatchSize(64)

      options.pipeline shouldBe stages
      options.fullDocument shouldBe FullDocument.DEFAULT
      options.fullDocumentBeforeChange shouldBe Some(FullDocumentBeforeChange.WHEN_AVAILABLE)
      options.startAtOperationTime shouldBe Some(BsonTimestamp(1, 1))
      options.maxAwaitTime shouldBe Some(2.seconds)
      options.batchSize shouldBe Some(64)
    }
  }
