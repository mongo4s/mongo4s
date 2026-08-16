package mongo4s.cats

import cats.effect.Resource
import cats.effect.kernel.Async
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridgeConfig}

import CatsInstances.given

object MongoClientResource:

  def fromClient[F[*]](client: RSMongoClient)(using F: Async[F], config: RsBridgeConfig): Resource[F, MongoClient[F, CatsStream[F]]] =
    Resource.make(
      MongoClient.fromClient[F, CatsStream[F]](client)
    )(_.close)

  def fromSettings[F[*]](settings: MongoClientSettings)(using F: Async[F], config: RsBridgeConfig): Resource[F, MongoClient[F, CatsStream[F]]] =
    Resource.make(
      MongoClient.fromSettings[F, CatsStream[F]](settings)
    )(_.close)

  def fromConnectionString[F[*]](connectionString: String)(using F: Async[F], config: RsBridgeConfig): Resource[F, MongoClient[F, CatsStream[F]]] =
    Resource.make(
      MongoClient.fromConnectionString[F, CatsStream[F]](connectionString)
    )(_.close)
