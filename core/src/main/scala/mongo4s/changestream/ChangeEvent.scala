package mongo4s.changestream

import scala.util.control.NonFatal

import org.bson.{BsonDateTime, BsonDocument, BsonTimestamp}
import com.mongodb.client.model.changestream.{ChangeStreamDocument, OperationType, SplitEvent, UpdateDescription}

import mongo4s.bson.{BsonError, DecodeResult}

final case class ChangeEvent[A](
    operationType: OperationType,
    namespace: Option[Namespace],
    destination: Option[Namespace],
    documentKey: Option[BsonDocument],
    fullDocument: Option[A],
    fullDocumentBeforeChange: Option[A],
    updateDescription: Option[UpdateDescription],
    resumeToken: BsonDocument,
    clusterTime: Option[BsonTimestamp],
    wallTime: Option[BsonDateTime],
    splitEvent: Option[SplitEvent],
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
      operationType = document.getOperationType,
      namespace = Namespace.fromDriver(document.getNamespaceDocument),
      destination = Namespace.fromDriver(document.getDestinationNamespaceDocument),
      documentKey = Option(document.getDocumentKey),
      fullDocument = full,
      fullDocumentBeforeChange = before,
      updateDescription = Option(document.getUpdateDescription),
      resumeToken = document.getResumeToken,
      clusterTime = Option(document.getClusterTime),
      wallTime = Option(document.getWallTime),
      splitEvent = Option(document.getSplitEvent),
    )

  private[mongo4s] def attempting[A](
      document: ChangeStreamDocument[BsonDocument],
      decode: BsonDocument => Either[BsonError, A],
  ): Either[BsonError, ChangeEvent[DecodeResult[A]]] =
    fromDriver(
      document,
      raw =>
        Right(
          try decode(raw)
          catch case NonFatal(error) => Left(BsonError.fromThrowable(error))
        ),
    )

  private def sequence[E, A](o: Option[Either[E, A]]): Either[E, Option[A]] =
    o match
      case None           => Right(None)
      case Some(Left(e))  => Left(e)
      case Some(Right(a)) => Right(Some(a))
