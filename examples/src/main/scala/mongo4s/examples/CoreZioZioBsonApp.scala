package mongo4s.examples

import java.time.Instant

import zio.*

import mongo4s.zio.ZioStream
import mongo4s.{Field, MongoClient}

import mongo4s.bson.BsonInstances.given
import mongo4s.bson.ziobson.ZioBsonInstances.given
import mongo4s.zio.ZioInstances.given
import zioBsonCodecs.given

object CoreZioZioBsonApp extends ZIOAppDefault:

  type S[A] = ZioStream[A]

  private def mongoClient(connectionString: String): ZIO[Scope, Throwable, MongoClient[Task, S]] =
    ZIO.acquireRelease(MongoClient.fromConnectionString[Task, S](connectionString))(_.close.orDie)

  def run: ZIO[Any, Throwable, Unit] =
    ZIO.scoped:
      for
        client <- mongoClient("mongodb://localhost:27018")
        db     <- client.getDatabase("mongo4s_examples")
        users  <- db.getCollection[User]("core_zio_ziobson_users")
        alice   = User(
                    UserId("1"),
                    "Alice",
                    "alice@example.com",
                    30,
                    Role.Admin,
                    Address("New York", "10001"),
                    List("vip", "beta"),
                    active = true,
                    Instant.now(),
                  )
        _      <- users.insertOne(alice)
        found  <- users.find(Field.of[User, UserId](_.id).equalTo(UserId("1"))).first
        _      <- Console.printLine(s"found: $found")
        adults <- users.find(Field.of[User, Int](_.age).gte(18)).all
        _      <- Console.printLine(s"adults: ${adults.size}")
      yield ()
