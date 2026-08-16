package mongo4s.internal

import mongo4s.RsBridgeError

private[mongo4s] object RsBridgeSupport:

  def selectOne[A](xs: List[A], strict: Boolean): Either[RsBridgeError, A] = xs match
    case Nil               => Left(RsBridgeError.EmptyResult())
    case a :: Nil          => Right(a)
    case a :: _ if !strict => Right(a)
    case many              => Left(RsBridgeError.TooManyResults(many.size))

  def selectOption[A](xs: List[A], strict: Boolean): Either[RsBridgeError, Option[A]] = xs match
    case Nil               => Right(None)
    case a :: Nil          => Right(Some(a))
    case a :: _ if !strict => Right(Some(a))
    case many              => Left(RsBridgeError.TooManyResults(many.size))
