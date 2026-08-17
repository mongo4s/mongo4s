package mongo4s.examples

import java.time.Instant

import rapid.Task

import mongo4s.Field
import mongo4s.rapid.MongoClientResource

import mongo4s.bson.BsonInstances.given
import mongo4s.bson.calypso.CalypsoInstances.given
import calypsoCodecs.given

object CoreRapidCalypsoApp:

  private val program: Task[Unit] =
    MongoClientResource.fromConnectionString("mongodb://localhost:27018"): client =>
      for
        db     <- client.getDatabase("mongo4s_examples")
        users  <- db.getCollection[User]("core_rapid_calypso_users")
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
        _      <- Task(println(s"found: $found"))
        adults <- users.find(Field.of[User, Int](_.age).gte(18)).all
        _      <- Task(println(s"adults: ${adults.size}"))
      yield ()

  def main(args: Array[String]): Unit = program.sync()
