package mongo4s.kyo

import kyo.{<, Abort, Async, Scope, Sync}
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridgeConfig}

import KyoInstances.given

object MongoClientResource:

  def fromClient(client: RSMongoClient)(using
      config: RsBridgeConfig
  ): MongoClient[KIO, KStream] < (Scope & Sync & Async & Abort[Throwable]) =
    Scope.acquireRelease(MongoClient.fromClient[KIO, KStream](client))(_.close)

  def fromSettings(settings: MongoClientSettings)(using
      config: RsBridgeConfig
  ): MongoClient[KIO, KStream] < (Scope & Sync & Async & Abort[Throwable]) =
    Scope.acquireRelease(MongoClient.fromSettings[KIO, KStream](settings))(_.close)

  def fromConnectionString(connectionString: String)(using
      config: RsBridgeConfig
  ): MongoClient[KIO, KStream] < (Scope & Sync & Async & Abort[Throwable]) =
    Scope.acquireRelease(MongoClient.fromConnectionString[KIO, KStream](connectionString))(_.close)
