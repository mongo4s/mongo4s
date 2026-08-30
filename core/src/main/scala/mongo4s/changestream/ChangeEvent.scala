package mongo4s.changestream

import org.bson.{BsonDocument, BsonTimestamp}
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType, UpdateDescription}

import mongo4s.bson.BsonError

final case class ChangeEvent[A](
    operationType: OperationType,
    documentKey: Option[BsonDocument],
    fullDocument: Option[A],
    fullDocumentBeforeChange: Option[A],
    updateDescription: Option[UpdateDescription],
    resumeToken: BsonDocument,
    clusterTime: Option[BsonTimestamp],
)

object ChangeEvent:
  private[mongo4s] def fromDriver[A](
      document: ChangeStreamDocument[BsonDocument],
      decode: BsonDocument => Either[BsonError, A],
  ): Either[BsonError, ChangeEvent[A]] =
    for
      full   <- sequence(Option(document.getFullDocument).map(decode))
      before <- sequence(Option(document.getFullDocumentBeforeChange).map(decode))
    yield ChangeEvent(
      document.getOperationType,
      Option(document.getDocumentKey),
      full,
      before,
      Option(document.getUpdateDescription),
      document.getResumeToken,
      Option(document.getClusterTime),
    )

  private def sequence[E, A](o: Option[Either[E, A]]): Either[E, Option[A]] =
    o match
      case None           => Right(None)
      case Some(Left(e))  => Left(e)
      case Some(Right(a)) => Right(Some(a))
