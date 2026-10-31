package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonBoolean, BsonDocument, BsonDouble, BsonInt32, BsonString}
import com.mongodb.connection.{ClusterId, ServerId}
import com.mongodb.bulk.{BulkWriteError, BulkWriteResult, WriteConcernError}
import com.mongodb.{
  ClientBulkWriteException,
  MongoBulkWriteException,
  MongoConnectionPoolClearedException,
  MongoCredential,
  MongoException,
  MongoExecutionTimeoutException,
  MongoNodeIsRecoveringException,
  MongoNotPrimaryException,
  MongoOperationTimeoutException,
  MongoSecurityException,
  MongoServerUnavailableException,
  MongoSocketOpenException,
  MongoTimeoutException,
  MongoWriteConcernException,
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

  private def commandFailure(code: Int, codeName: String): BsonDocument =
    BsonDocument("ok", BsonDouble(0)).append("code", BsonInt32(code)).append("codeName", BsonString(codeName)).append("errmsg", BsonString(codeName))

  private val writeConcernTimeout =
    WriteConcernError(64, "WriteConcernFailed", "waiting for replication timed out", BsonDocument("wtimeout", BsonBoolean.TRUE))

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

    "name credentials the server refused, which the driver reports as a client-side security exception" in {
      val credential = MongoCredential.createCredential("app", "admin", "wrong".toCharArray)

      MongoError.translate(MongoSecurityException(credential, "Exception authenticating")) shouldBe a[MongoError.Unauthorized]
    }

    "name an execution timeout, whether it arrives as a category or as its own exception" in {
      MongoError.translate(writeError(50, "operation exceeded time limit")) shouldBe a[MongoError.ExecutionTimeout]
      MongoError.translate(MongoExecutionTimeoutException(50, "exceeded")) shouldBe a[MongoError.ExecutionTimeout]
    }

    "name an operation whose timeoutMS ran out an execution timeout, although the driver's class extends the selection timeout" in {
      MongoError.translate(MongoOperationTimeoutException("Operation exceeded the timeout limit")) shouldBe a[MongoError.ExecutionTimeout]
    }

    "not re-commit a transaction whose commit ran out of timeoutMS" in {
      MongoSession.exceededMaxTime(MongoOperationTimeoutException("Operation exceeded the timeout limit")) shouldBe true
      MongoSession.exceededMaxTime(MongoTimeoutException("no server available")) shouldBe false
    }

    "name an unreachable server, whether the socket or server selection failed" in {
      MongoError.translate(MongoSocketOpenException("refused", address)) shouldBe a[MongoError.Unavailable]
      MongoError.translate(MongoTimeoutException("no server available")) shouldBe a[MongoError.Unavailable]
    }

    "name a primary that stepped down, a node recovering, a cleared pool and a closed server unavailable" in {
      val server = ServerId(ClusterId(), address)

      MongoError.translate(MongoNotPrimaryException(commandFailure(10107, "NotWritablePrimary"), address)) shouldBe a[MongoError.Unavailable]
      MongoError.translate(MongoNodeIsRecoveringException(commandFailure(91, "ShutdownInProgress"), address)) shouldBe a[MongoError.Unavailable]
      MongoError.translate(MongoConnectionPoolClearedException(server, null)) shouldBe a[MongoError.Unavailable]
      MongoError.translate(MongoServerUnavailableException("The server is closed")) shouldBe a[MongoError.Unavailable]
    }

    "carry the write-concern failure of a single write, with its code" in {
      val translated = MongoError.translate(MongoWriteConcernException(writeConcernTimeout, null, address, java.util.List.of())).asInstanceOf[MongoError]

      translated.code shouldBe 64
      translated.writeConcernErrors shouldBe List(MongoError.WriteConcernFailure(64, "waiting for replication timed out"))
    }

    "report a bulk write that failed only on its write concern, rather than as a failure with nothing in it" in {
      val bulk = MongoBulkWriteException(BulkWriteResult.unacknowledged(), java.util.List.of(), writeConcernTimeout, address, Set.empty[String].asJava)

      MongoError.translate(bulk) match
        case failed: MongoError.BulkWriteFailed =>
          failed.failures shouldBe Nil
          failed.writeConcernErrors shouldBe List(MongoError.WriteConcernFailure(64, "waiting for replication timed out"))
        case other                              => fail(s"expected a BulkWriteFailed, got $other")
    }

    "keep a bulk write's write-concern failure when the batches are stitched back together" in {
      val bulk = MongoBulkWriteException(BulkWriteResult.unacknowledged(), java.util.List.of(), writeConcernTimeout, address, Set.empty[String].asJava)

      MongoError.bulkWriteFailed(bulk).after(10, mongo4s.results.BulkWriteResult.none).writeConcernErrors.map(_.code) shouldBe List(64)
    }

    "list every write-concern failure of a client bulk write" in {
      val client = ClientBulkWriteException(null, java.util.List.of(writeConcernTimeout), java.util.Map.of(), null, address)

      MongoError.translate(client).asInstanceOf[MongoError].writeConcernErrors.map(_.code) shouldBe List(64)
    }

    "report no write-concern failure for an error that carries none" in {
      MongoError.translate(writeError(11000, "dup")).asInstanceOf[MongoError].writeConcernErrors shouldBe Nil
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

    "list a client bulk write's failures by position, in order, with no partial result when the driver kept none" in {
      val client = ClientBulkWriteException(
        MongoException("client bulk write failed"),
        java.util.List.of(),
        Map(Integer.valueOf(3) -> WriteError(121, "validation", BsonDocument()), Integer.valueOf(1) -> WriteError(11000, "dup", BsonDocument())).asJava,
        null,
        address,
      )

      MongoError.translate(client) match
        case failed: MongoError.ClientBulkWriteFailed =>
          failed.failures.map(_.index) shouldBe List(1, 3)
          failed.duplicateKeys.map(_.index) shouldBe List(1)
          failed.partialResult shouldBe None
        case other                                    => fail(s"expected a ClientBulkWriteFailed, got $other")
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
