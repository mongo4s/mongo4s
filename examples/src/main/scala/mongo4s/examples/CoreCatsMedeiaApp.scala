package mongo4s.examples

import java.time.Instant

import cats.effect.{IO, IOApp}

import mongo4s.Field
import mongo4s.cats.{CatsStream, MongoClientResource}

import mongo4s.bson.BsonInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given
import medeiaCodecs.given

object CoreCatsMedeiaApp extends IOApp.Simple:

  type S[A] = CatsStream[IO][A]

  def run: IO[Unit] =
    MongoClientResource
      .fromConnectionString[IO]("mongodb://localhost:27018")
      .use { client =>
        for
          db    <- client.getDatabase("mongo4s_examples")
          users <- db.getCollection[User]("core_cats_medeia_users")

          alice = User(
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
          _      <- IO.println(s"found: $found")
          adults <- users.find(Field.of[User, Int](_.age).gte(18)).all
          _      <- IO.println(s"adults: ${adults.size}")
        yield ()
      }
