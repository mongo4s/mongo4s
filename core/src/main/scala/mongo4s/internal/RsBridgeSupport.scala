package mongo4s.internal

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import mongo4s.{MongoError, RsBridgeError}

private[mongo4s] object RsBridgeSupport:

  val SingleResultProbe: Int = 2

  def translating[A](publisher: Publisher[A]): Publisher[A] =
    (subscriber: Subscriber[? >: A]) =>
      publisher.subscribe(
        new Subscriber[A]:
          def onSubscribe(subscription: Subscription): Unit = subscriber.onSubscribe(subscription)
          def onNext(value: A): Unit                        = subscriber.onNext(value)
          def onError(error: Throwable): Unit               = subscriber.onError(MongoError.translate(error))
          def onComplete(): Unit                            = subscriber.onComplete()
      )

  def selectOne[A](xs: List[A], strict: Boolean): Either[RsBridgeError, A] =
    xs match
      case Nil               => Left(RsBridgeError.EmptyResult())
      case a :: Nil          => Right(a)
      case a :: _ if !strict => Right(a)
      case _                 => Left(RsBridgeError.TooManyResults())

  def selectOption[A](xs: List[A], strict: Boolean): Either[RsBridgeError, Option[A]] =
    xs match
      case Nil               => Right(None)
      case a :: Nil          => Right(Some(a))
      case a :: _ if !strict => Right(Some(a))
      case _                 => Left(RsBridgeError.TooManyResults())
