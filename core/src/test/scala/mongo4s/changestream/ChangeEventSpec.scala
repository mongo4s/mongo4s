package mongo4s.changestream

import java.util.Collections

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDateTime, BsonDocument, BsonInt32, BsonString}
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType, SplitEvent, UpdateDescription}

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
      wallTime: BsonDateTime = null,
      splitEvent: SplitEvent = null,
      namespace: BsonDocument = null,
      destination: BsonDocument = null,
  ): ChangeStreamDocument[BsonDocument] =
    ChangeStreamDocument[BsonDocument](
      operationType,
      BsonDocument("_data", BsonString("resume-token")),
      namespace,
      null,
      destination,
      fullDocument,
      fullDocumentBeforeChange,
      documentKey,
      null,
      updateDescription,
      null,
      null,
      wallTime,
      splitEvent,
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
          namespace = None,
          destination = None,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = Some(Person("bob", 30)),
          fullDocumentBeforeChange = None,
          updateDescription = None,
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
          wallTime = None,
          splitEvent = None,
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
          namespace = None,
          destination = None,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = Some(Person("bob", 31)),
          fullDocumentBeforeChange = None,
          updateDescription = Some(update),
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
          wallTime = None,
          splitEvent = None,
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
          namespace = None,
          destination = None,
          documentKey = Some(BsonDocument("_id", BsonString("1"))),
          fullDocument = None,
          fullDocumentBeforeChange = None,
          updateDescription = None,
          resumeToken = BsonDocument("_data", BsonString("resume-token")),
          clusterTime = None,
          wallTime = None,
          splitEvent = None,
        )
      )
    }

    "carry the wall time and the split marker of a large event" in {
      val document = changeStreamDocument(
        operationType = "insert",
        fullDocument = personDocument("bob", 30),
        wallTime = BsonDateTime(1700000000000L),
        splitEvent = SplitEvent(1, 3),
      )

      val result = ChangeEvent.fromDriver(document, decodePerson)

      result.map(_.wallTime) shouldBe Right(Some(BsonDateTime(1700000000000L)))
      result.map(_.splitEvent.map(_.getFragment)) shouldBe Right(Some(1))
      result.map(_.splitEvent.map(_.getOf)) shouldBe Right(Some(3))
    }

    "name the collection an event happened in, and where a rename moved it to" in {
      val document = changeStreamDocument(
        operationType = "rename",
        namespace = BsonDocument("db", BsonString("shop")).append("coll", BsonString("orders")),
        destination = BsonDocument("db", BsonString("shop")).append("coll", BsonString("archive")),
      )

      val result = ChangeEvent.fromDriver(document, decodePerson)

      result.map(_.namespace) shouldBe Right(Some(Namespace("shop", Some("orders"))))
      result.map(_.destination) shouldBe Right(Some(Namespace("shop", Some("archive"))))
    }

    "name only the database for an event about the whole database" in {
      val document = changeStreamDocument(operationType = "dropDatabase", namespace = BsonDocument("db", BsonString("shop")))

      ChangeEvent.fromDriver(document, decodePerson).map(_.namespace) shouldBe Right(Some(Namespace("shop", None)))
    }

    "surface a decode failure on a malformed full document" in {
      val document = changeStreamDocument(
        operationType = "insert",
        fullDocument = BsonDocument("name", BsonString("bob")), // missing "age"
      )
      ChangeEvent.fromDriver(document, decodePerson) shouldBe Left(BsonError.MissingField("age"))
    }
  }

  "ChangeEvent.attempting" should {

    "keep the event, and the resume token a consumer checkpoints, when its document does not decode" in {
      val document = changeStreamDocument(
        operationType = "insert",
        fullDocument = BsonDocument("name", BsonString("bob")),
        documentKey = BsonDocument("_id", BsonString("1")),
      )

      val result = ChangeEvent.attempting(document, decodePerson)

      result.map(_.resumeToken) shouldBe Right(BsonDocument("_data", BsonString("resume-token")))
      result.map(_.documentKey) shouldBe Right(Some(BsonDocument("_id", BsonString("1"))))
      result.map(_.fullDocument) shouldBe Right(Some(Left(BsonError.MissingField("age"))))
    }

    "report a decoder that throws as a Left on the document, not a failed event" in {
      val document = changeStreamDocument(operationType = "insert", fullDocument = BsonDocument())

      ChangeEvent.attempting[Person](document, _ => throw IllegalStateException("boom")).map(_.fullDocument.exists(_.isLeft)) shouldBe Right(true)
    }
  }
