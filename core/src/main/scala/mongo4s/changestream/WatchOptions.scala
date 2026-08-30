package mongo4s.changestream

import scala.concurrent.duration.FiniteDuration

import org.bson.{BsonDocument, BsonTimestamp}
import com.mongodb.client.model.changestream.{FullDocument, FullDocumentBeforeChange}

import mongo4s.operations.Stage

final case class WatchOptions[E](
    pipeline: Seq[Stage[E]] = Seq.empty[Stage[E]],
    fullDocument: FullDocument = FullDocument.UPDATE_LOOKUP,
    fullDocumentBeforeChange: Option[FullDocumentBeforeChange] = None,
    resumeAfter: Option[BsonDocument] = None,
    startAfter: Option[BsonDocument] = None,
    startAtOperationTime: Option[BsonTimestamp] = None,
    maxAwaitTime: Option[FiniteDuration] = None,
    batchSize: Option[Int] = None,
):
  def withPipeline(stages: Seq[Stage[E]]): WatchOptions[E] =
    copy(pipeline = stages)

  def withFullDocument(value: FullDocument): WatchOptions[E] =
    copy(fullDocument = value)

  def withFullDocumentBeforeChange(value: FullDocumentBeforeChange): WatchOptions[E] =
    copy(fullDocumentBeforeChange = Some(value))

  def resumingAfter(token: BsonDocument): WatchOptions[E] =
    copy(resumeAfter = Some(token), startAfter = None)

  def startingAfter(token: BsonDocument): WatchOptions[E] =
    copy(startAfter = Some(token), resumeAfter = None)

  def startingAt(time: BsonTimestamp): WatchOptions[E] =
    copy(startAtOperationTime = Some(time))

  def withMaxAwaitTime(value: FiniteDuration): WatchOptions[E] =
    copy(maxAwaitTime = Some(value))

  def withBatchSize(value: Int): WatchOptions[E] =
    copy(batchSize = Some(value))

object WatchOptions:
  def default[E]: WatchOptions[E] = WatchOptions()

  def resumeAfter[E](token: BsonDocument): WatchOptions[E] = default[E].resumingAfter(token)
