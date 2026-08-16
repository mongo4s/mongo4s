package mongo4s.changestream

import java.util.Collections

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonString}
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType, UpdateDescription}

import mongo4s.bson.{BsonDecoder, BsonError}

object ChangeEventSpec:

  final case class Person(name: String, age: Int)

  def decodePerson(document: BsonDocument): Either[BsonError, Person] =
    for
      name <- Option(document.get("name")).toRight(BsonError.MissingField("name")).flatMap(BsonDecoder[String].decode)
      age  <- Option(document.get("age")).toRight(BsonError.MissingField("age")).flatMap(BsonDecoder[Int].decode)
    yield Person(name, age)

  private def personDocument(name: String, age: Int): BsonDocument =
    BsonDocument().append("name", BsonString(name)).append("age", BsonInt32(age))

  def changeStreamDocument(
      operationType: String,
      fullDocument: BsonDocument = null,
      fullDocumentBeforeChange: BsonDocument = null,
      documentKey: BsonDocument = null,
      updateDescription: UpdateDescription = null,
  ): ChangeStreamDocument[BsonDocument] =
    ChangeStreamDocument[BsonDocument](
      operationType,
      BsonDocument("_data", BsonString("resume-token")),
      null,
      null,
      null,
      fullDocument,
      fullDocumentBeforeChange,
      documentKey,
      null,
      updateDescription,
      null,
      null,
      null,
      null,
      null,
    )

final class ChangeEventSpec extends AnyWordSpec, Matchers:
  import ChangeEventSpec.*

  "ChangeEvent.fromDriver" should {

    "decode an insert event with a full document" in {
      val document = changeStreamDocument(
        operationType = "insert",
        fullDocument = personDocument("bob", 30),
        documentKey = BsonDocument("_id", BsonString("1")),
      )
      val result   = ChangeEvent.fromDriver(document, decodePerson)
      result shouldBe Right(
        ChangeEvent(
          operationType = OperationType.INSERT,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = Some(Person("bob", 30)),
          fullDocumentBeforeChange = None,
          updateDescription = None,
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
        )
      )
    }

    "decode an update event with updateDescription and a looked-up full document" in {
      val update   = UpdateDescription(Collections.emptyList(), BsonDocument("age", BsonInt32(31)))
      val document = changeStreamDocument(
        operationType = "update",
        fullDocument = personDocument("bob", 31),
        documentKey = BsonDocument("_id", BsonString("1")),
        updateDescription = update,
      )
      val result   = ChangeEvent.fromDriver(document, decodePerson)
      result shouldBe Right(
        ChangeEvent(
          operationType = OperationType.UPDATE,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = Some(Person("bob", 31)),
          fullDocumentBeforeChange = None,
          updateDescription = Some(update),
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
        )
      )
    }

    "decode a delete event with no full document" in {
      val document = changeStreamDocument(
        operationType = "delete",
        documentKey = BsonDocument("_id", BsonString("1")),
      )
      val result   = ChangeEvent.fromDriver(document, decodePerson)
      result shouldBe Right(
        ChangeEvent(
          operationType = OperationType.DELETE,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = None,
          fullDocumentBeforeChange = None,
          updateDescription = None,
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
        )
      )
    }

    "surface a decode failure on a malformed full document" in {
      val document = changeStreamDocument(
        operationType = "insert",
        fullDocument = BsonDocument("name", BsonString("bob")), // missing "age"
      )
      ChangeEvent.fromDriver(document, decodePerson) shouldBe Left(BsonError.MissingField("age"))
    }
  }
