package mongo4s.internal

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.{Failure, Success, Try}

import org.reactivestreams.{Publisher, Subscriber, Subscription}

private[mongo4s] final class PublisherIterator[A](publisher: Publisher[A], bufferSize: Int) extends Iterator[A]:

  private val capacity = math.max(1, bufferSize)

  private val queue = ArrayBlockingQueue[Try[Option[A]]](capacity + 1)

  private val subscriptionRef = AtomicReference[Subscription]()
  private val terminated      = AtomicBoolean(false)
  private val cancelled       = AtomicBoolean(false)

  private var pending   = Option.empty[A]
  private var exhausted = false

  publisher.subscribe(
    new Subscriber[A]:
      def onSubscribe(subscription: Subscription): Unit =
        subscriptionRef.set(subscription)
        if cancelled.get
        then subscription.cancel()
        else subscription.request(capacity.toLong)
      end onSubscribe

      def onNext(value: A): Unit =
        if !terminated.get && !cancelled.get
        then queue.put(Success(Some(value)))

      def onError(error: Throwable): Unit =
        if terminated.compareAndSet(false, true)
        then queue.put(Failure(error))

      def onComplete(): Unit =
        if terminated.compareAndSet(false, true)
        then queue.put(Success(None))
  )

  def cancel(): Unit =
    cancelled.set(true)

    val subscription = subscriptionRef.getAndSet(null)

    if subscription ne null
    then subscription.cancel()
  end cancel

  def hasNext: Boolean =
    if pending.isDefined
    then true
    else if exhausted
    then false
    else
      queue.take() match
        case Success(Some(value)) =>
          pending = Some(value)
          val subscription = subscriptionRef.get
          if subscription ne null
          then subscription.request(1L)
          true

        case Success(None) =>
          exhausted = true
          false

        case Failure(error) =>
          exhausted = true
          throw error
  end hasNext

  def next(): A =
    if !hasNext
    then throw new NoSuchElementException("next() on an exhausted publisher")
    else
      val value = pending
      pending = None
      value.get
  end next
