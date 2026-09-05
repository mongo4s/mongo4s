package mongo4s.changestream

import scala.concurrent.duration.FiniteDuration

import org.bson.{BsonDocument, BsonTimestamp}
import com.mongodb.client.model.changestream.{FullDocument, FullDocumentBeforeChange}

import mongo4s.operations.Stage

/** How a change stream is opened. Built by chaining from `default`, never constructed directly, so an option added later stays binary-compatible.
  */
final class WatchOptions[E] private (
    val pipeline: Seq[Stage[E]],
    val fullDocument: FullDocument,
    val fullDocumentBeforeChange: Option[FullDocumentBeforeChange],
    val resumeAfter: Option[BsonDocument],
    val startAfter: Option[BsonDocument],
    val startAtOperationTime: Option[BsonTimestamp],
    val maxAwaitTime: Option[FiniteDuration],
    val batchSize: Option[Int],
    val expandedEvents: Boolean,
):
  def withPipeline(stages: Seq[Stage[E]]): WatchOptions[E] =
    copy(pipeline = stages)

  def withFullDocument(value: FullDocument): WatchOptions[E] =
    copy(fullDocument = value)

  def withFullDocumentBeforeChange(value: FullDocumentBeforeChange): WatchOptions[E] =
    copy(fullDocumentBeforeChange = Some(value))

  def resumingAfter(token: BsonDocument): WatchOptions[E] =
    copy(resumeAfter = Some(token), startAfter = None, startAtOperationTime = None)

  def startingAfter(token: BsonDocument): WatchOptions[E] =
    copy(startAfter = Some(token), resumeAfter = None, startAtOperationTime = None)

  def startingAt(time: BsonTimestamp): WatchOptions[E] =
    copy(startAtOperationTime = Some(time), resumeAfter = None, startAfter = None)

  def withMaxAwaitTime(value: FiniteDuration): WatchOptions[E] =
    copy(maxAwaitTime = Some(value))

  def withBatchSize(value: Int): WatchOptions[E] =
    copy(batchSize = Some(value))

  /** The driver's `showExpandedEvents`: DDL events — `createIndexes`, `drop`, `rename` and the rest — are reported alongside the document ones. Needs MongoDB 6.0.
    */
  def withExpandedEvents: WatchOptions[E] =
    copy(expandedEvents = true)

  private def copy(
      pipeline: Seq[Stage[E]] = pipeline,
      fullDocument: FullDocument = fullDocument,
      fullDocumentBeforeChange: Option[FullDocumentBeforeChange] = fullDocumentBeforeChange,
      resumeAfter: Option[BsonDocument] = resumeAfter,
      startAfter: Option[BsonDocument] = startAfter,
      startAtOperationTime: Option[BsonTimestamp] = startAtOperationTime,
      maxAwaitTime: Option[FiniteDuration] = maxAwaitTime,
      batchSize: Option[Int] = batchSize,
      expandedEvents: Boolean = expandedEvents,
  ): WatchOptions[E] =
    new WatchOptions[E](
      pipeline = pipeline,
      fullDocument = fullDocument,
      fullDocumentBeforeChange = fullDocumentBeforeChange,
      resumeAfter = resumeAfter,
      startAfter = startAfter,
      startAtOperationTime = startAtOperationTime,
      maxAwaitTime = maxAwaitTime,
      batchSize = batchSize,
      expandedEvents = expandedEvents,
    )

object WatchOptions:
  def default[E]: WatchOptions[E] =
    new WatchOptions[E](
      pipeline = Seq.empty[Stage[E]],
      fullDocument = FullDocument.UPDATE_LOOKUP,
      fullDocumentBeforeChange = None,
      resumeAfter = None,
      startAfter = None,
      startAtOperationTime = None,
      maxAwaitTime = None,
      batchSize = None,
      expandedEvents = false,
    )

  def resumeAfter[E](token: BsonDocument): WatchOptions[E] = default[E].resumingAfter(token)
