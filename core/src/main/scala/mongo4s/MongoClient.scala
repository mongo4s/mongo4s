package mongo4s

import org.bson.BsonDocument
import com.mongodb.MongoClientSettings
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient as RSMongoClient, MongoClients}

import mongo4s.bson.BsonDocumentDecoder
import mongo4s.changestream.ChangeEvent
import mongo4s.internal.MongoClientImpl

trait MongoClient[F[*], S[*]]:
  def getDatabase(name: String): F[MongoDatabase[F, S]]
  def startSession: F[ClientSession]
  def listDatabaseNames(using session: Option[ClientSession] = None)(using Streamable[S, String]): S[String]
  def listDatabases(using session: Option[ClientSession] = None)(using Streamable[S, BsonDocument]): S[BsonDocument]

  def watch(pipeline: Seq[BsonDocument] = Seq.empty, fullDocument: FullDocument = FullDocument.UPDATE_LOOKUP)(using
      session: Option[ClientSession] = None
  )(using Streamable[S, ChangeEvent[BsonDocument]]): S[ChangeEvent[BsonDocument]]

  def watchAs[A](pipeline: Seq[BsonDocument] = Seq.empty, fullDocument: FullDocument = FullDocument.UPDATE_LOOKUP)(using
      session: Option[ClientSession] = None
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]]
  
  def withTransaction[A](fa: Option[ClientSession] ?=> F[A])(using F: Effect[F], rs: RsBridge[F, S]): F[A] =
    F.flatMap(startSession) { session =>
      def closeAfter(fb: F[A]): F[A] =
        F.handleErrorWith(F.flatMap(fb)(a => F.map(F.delay(session.close()))(_ => a))) { ex =>
          F.flatMap(F.delay(session.close()))(_ => F.raiseError(ex))
        }

      closeAfter(session.withTransaction[F, S, A](fa))
    }

  def close: F[Unit]

  def underlying: RSMongoClient

object MongoClient:

  def fromClient[F[*], S[*]](client: RSMongoClient)(using F: Effect[F], rs: RsBridge[F, S]): F[MongoClient[F, S]] =
    F.pure(MongoClientImpl(client))

  def fromSettings[F[*], S[*]](settings: MongoClientSettings)(using F: Effect[F], rs: RsBridge[F, S]): F[MongoClient[F, S]] =
    F.delay(MongoClientImpl(MongoClients.create(settings)))

  def fromConnectionString[F[*], S[*]](connectionString: String)(using F: Effect[F], rs: RsBridge[F, S]): F[MongoClient[F, S]] =
    F.delay(MongoClientImpl(MongoClients.create(connectionString)))
