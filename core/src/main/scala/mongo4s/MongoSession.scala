package mongo4s

import com.mongodb.reactivestreams.client.ClientSession

object MongoSession:
  def startTransaction[F[*]](session: ClientSession)(using F: Effect[F]): F[Unit] =
    F.delay(session.startTransaction())

  def commitTransaction[F[*], S[*]](session: ClientSession)(using rs: RsBridge[F, S]): F[Unit] =
    rs.unit(session.commitTransaction())

  def abortTransaction[F[*], S[*]](session: ClientSession)(using rs: RsBridge[F, S]): F[Unit] =
    rs.unit(session.abortTransaction())

extension (session: ClientSession)
  
  def withTransaction[F[*], S[*], A](fa: Option[ClientSession] ?=> F[A])(using F: Effect[F], rs: RsBridge[F, S]): F[A] =
    given Option[ClientSession] = Some(session)
    F.flatMap(MongoSession.startTransaction[F](session)) { _ =>
      F.handleErrorWith(F.flatMap(fa)(a => F.map(MongoSession.commitTransaction[F, S](session))(_ => a))) { ex =>
        F.flatMap(MongoSession.abortTransaction[F, S](session))(_ => F.raiseError(ex))
      }
    }
