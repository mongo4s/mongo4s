package mongo4s.rapid

import rapid.Task
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{MongoClient, RsBridgeConfig}

import RapidInstances.given

object MongoClientResource:

  def fromClient[A](client: RSMongoClient)(use: MongoClient[Task, RapidStream] => Task[A])(using config: RsBridgeConfig): Task[A] =
    MongoClient.fromClient[Task, RapidStream](client).flatMap(c => use(c).guarantee(c.close))

  def fromSettings[A](settings: MongoClientSettings)(use: MongoClient[Task, RapidStream] => Task[A])(using config: RsBridgeConfig): Task[A] =
    MongoClient.fromSettings[Task, RapidStream](settings).flatMap(c => use(c).guarantee(c.close))

  def fromConnectionString[A](connectionString: String)(use: MongoClient[Task, RapidStream] => Task[A])(using
      config: RsBridgeConfig
  ): Task[A] =
    MongoClient.fromConnectionString[Task, RapidStream](connectionString).flatMap(c => use(c).guarantee(c.close))
