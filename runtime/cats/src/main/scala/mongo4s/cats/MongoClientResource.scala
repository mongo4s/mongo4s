package mongo4s.cats

import scala.concurrent.duration.FiniteDuration

import cats.effect.kernel.Async
import cats.effect.{Ref, Resource}
import fs2.Stream
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridge, RsBridgeConfig}

import CatsInstances.given
import cats.effect.syntax.spawn.given

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

  def reachability[F[*], S[*]](client: MongoClient[F, S], interval: FiniteDuration)(using F: Async[F], rs: RsBridge[F, S]): Resource[F, F[Boolean]] =
    for
      reachable <- Resource.eval(Ref.of[F, Boolean](false))
      check      = F.flatMap(client.ping(interval))(reachable.set)
      _         <- Resource.eval(check)
      _         <- Stream.awakeEvery[F](interval).evalMap(_ => check).compile.drain.background
    yield reachable.get
