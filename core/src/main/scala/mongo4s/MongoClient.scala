package mongo4s

import org.bson.BsonDocument
import com.mongodb.reactivestreams.client.{MongoClient as RSMongoClient, MongoClients}

import mongo4s.internal.MongoClientImpl

trait MongoClient[F[*], S[*]]:
  def getDatabase(name: String): F[MongoDatabase[F, S]]

  def listDatabaseNames(using Streamable[S, String]): S[String]
  def listDatabases(using Streamable[S, BsonDocument]): S[BsonDocument]

  def watch(using Streamable[S, BsonDocument]): S[BsonDocument]
  def watch(pipeline: Seq[BsonDocument])(using Streamable[S, BsonDocument]): S[BsonDocument]

  def close: F[Unit]

  def underlying: RSMongoClient

object MongoClient:
  def fromConnectionString[F[*], S[*]](connectionString: String)(using F: Effect[F], rs: RsBridge[F, S]): F[MongoClient[F, S]] =
    F.delay(MongoClientImpl(MongoClients.create(connectionString)))

  def fromClient[F[*], S[*]](client: RSMongoClient)(using Effect[F], RsBridge[F, S]): MongoClient[F, S] =
    MongoClientImpl(client)
