package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonDocument
import com.mongodb.bulk.{BulkWriteError, BulkWriteResult}
import com.mongodb.{
  MongoBulkWriteException,
  MongoException,
  MongoExecutionTimeoutException,
  MongoSocketOpenException,
  MongoTimeoutException,
  MongoWriteException,
  ServerAddress,
  WriteError,
}

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonError

final class MongoErrorSpec extends AnyWordSpec, Matchers:

  private val address = ServerAddress()

  private def writeError(code: Int, message: String): MongoWriteException =
    MongoWriteException(WriteError(code, message, BsonDocument()), address, java.util.Collections.emptyList())

  "MongoError.translate" should {

    "read a duplicate key off the driver's own category rather than a hard-coded list" in {
      MongoError.translate(writeError(11000, "E11000 duplicate key")) shouldBe a[MongoError.DuplicateKey]
      MongoError.translate(writeError(11001, "E11001 duplicate key")) shouldBe a[MongoError.DuplicateKey]
    }

    "name a write conflict, the error a transaction retries on" in {
      MongoError.translate(writeError(112, "WriteConflict")) shouldBe a[MongoError.WriteConflict]
    }

    "name an authorisation failure" in {
      MongoError.translate(writeError(13, "Unauthorized")) shouldBe a[MongoError.Unauthorized]
      MongoError.translate(MongoException(18, "AuthenticationFailed")) shouldBe a[MongoError.Unauthorized]
    }

    "name an execution timeout, whether it arrives as a category or as its own exception" in {
      MongoError.translate(writeError(50, "operation exceeded time limit")) shouldBe a[MongoError.ExecutionTimeout]
      MongoError.translate(MongoExecutionTimeoutException(50, "exceeded")) shouldBe a[MongoError.ExecutionTimeout]
    }

    "name an unreachable server, whether the socket or server selection failed" in {
      MongoError.translate(MongoSocketOpenException("refused", address)) shouldBe a[MongoError.Unavailable]
      MongoError.translate(MongoTimeoutException("no server available")) shouldBe a[MongoError.Unavailable]
    }

    "fall back to Failed for a code it does not model, without losing the code" in {
      val translated = MongoError.translate(writeError(9999, "something else"))

      translated shouldBe a[MongoError.Failed]
      translated.asInstanceOf[MongoError].code shouldBe 9999
    }

    "keep every failure of a bulk write rather than collapsing it to the first" in {
      val bulk = MongoBulkWriteException(
        BulkWriteResult.unacknowledged(),
        List(
          BulkWriteError(11000, "dup", BsonDocument(), 0),
          BulkWriteError(121, "validation", BsonDocument(), 2),
        ).asJava,
        null,
        address,
        Set.empty[String].asJava,
      )

      MongoError.translate(bulk) match
        case failed: MongoError.BulkWriteFailed =>
          failed.failures.map(_.index) shouldBe List(0, 2)
          failed.duplicateKeys.map(_.index) shouldBe List(0)
        case other                              => fail(s"expected a BulkWriteFailed, got $other")
    }

    "keep the driver's exception as the cause, and its labels" in {
      val driver = MongoException(112, "WriteConflict")
      driver.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)

      val translated = MongoError.translate(driver).asInstanceOf[MongoError]

      translated.cause shouldBe driver
      translated.getCause shouldBe driver
      translated.hasLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) shouldBe true
      translated.labels shouldBe Set(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
    }

    "leave alone anything that is not a driver failure" in {
      val decoding = BsonError.DecodingFailure(BsonError.MissingField("name"))
      val bridge   = RsBridgeError.EmptyResult()

      MongoError.translate(decoding) shouldBe decoding
      MongoError.translate(bridge) shouldBe bridge
    }

    "not wrap an error it has already translated" in {
      val once  = MongoError.translate(writeError(11000, "dup"))
      val twice = MongoError.translate(once)

      twice shouldBe theSameInstanceAs(once)
    }
  }
