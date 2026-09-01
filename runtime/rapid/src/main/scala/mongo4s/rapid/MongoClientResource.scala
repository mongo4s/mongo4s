package mongo4s.rapid

import rapid.Task
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridgeConfig}

import RapidInstances.given

object MongoClientResource:

  private def withClient[A](
      acquire: Task[MongoClient[Task, RapidStream]]
  )(use: MongoClient[Task, RapidStream] => Task[A]): Task[A] =
    acquire.flatMap(client => Task(use(client)).flatMap(identity).guarantee(client.close))

  def fromClient[A](client: RSMongoClient)(use: MongoClient[Task, RapidStream] => Task[A])(using config: RsBridgeConfig): Task[A] =
    withClient(MongoClient.fromClient[Task, RapidStream](client))(use)

  def fromSettings[A](settings: MongoClientSettings)(use: MongoClient[Task, RapidStream] => Task[A])(using config: RsBridgeConfig): Task[A] =
    withClient(MongoClient.fromSettings[Task, RapidStream](settings))(use)

  def fromConnectionString[A](connectionString: String)(use: MongoClient[Task, RapidStream] => Task[A])(using
      config: RsBridgeConfig
  ): Task[A] =
    withClient(MongoClient.fromConnectionString[Task, RapidStream](connectionString))(use)
