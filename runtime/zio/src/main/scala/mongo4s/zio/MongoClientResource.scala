package mongo4s.zio

import zio.{Scope, Task, ZIO}
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridgeConfig}

import ZioInstances.given

object MongoClientResource:

  def fromClient(client: RSMongoClient)(using config: RsBridgeConfig): ZIO[Scope, Throwable, MongoClient[Task, ZioStream]] =
    ZIO.acquireRelease(
      MongoClient.fromClient[Task, ZioStream](client)
    )(_.close.orDie)

  def fromSettings(settings: MongoClientSettings)(using config: RsBridgeConfig): ZIO[Scope, Throwable, MongoClient[Task, ZioStream]] =
    ZIO.acquireRelease(
      MongoClient.fromSettings[Task, ZioStream](settings)
    )(_.close.orDie)

  def fromConnectionString(connectionString: String)(using config: RsBridgeConfig): ZIO[Scope, Throwable, MongoClient[Task, ZioStream]] =
    ZIO.acquireRelease(
      MongoClient.fromConnectionString[Task, ZioStream](connectionString)
    )(_.close.orDie)
