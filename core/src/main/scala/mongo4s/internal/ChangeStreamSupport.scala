package mongo4s.internal

import java.util.concurrent.TimeUnit

import com.mongodb.reactivestreams.client.ChangeStreamPublisher

import mongo4s.changestream.WatchOptions

private[mongo4s] object ChangeStreamSupport:

  def configure[E, T](publisher: ChangeStreamPublisher[T], options: WatchOptions[E]): ChangeStreamPublisher[T] =
    publisher.fullDocument(options.fullDocument)
    options.fullDocumentBeforeChange.foreach(publisher.fullDocumentBeforeChange)
    options.resumeAfter.foreach(publisher.resumeAfter)
    options.startAfter.foreach(publisher.startAfter)
    options.startAtOperationTime.foreach(publisher.startAtOperationTime)
    options.maxAwaitTime.foreach(duration => publisher.maxAwaitTime(duration.toMillis, TimeUnit.MILLISECONDS))
    options.batchSize.foreach(publisher.batchSize)
    publisher
