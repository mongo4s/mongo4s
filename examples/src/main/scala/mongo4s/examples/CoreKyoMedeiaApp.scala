package mongo4s.examples

import java.time.Instant

import kyo.*

import mongo4s.Field
import mongo4s.kyo.MongoClientResource

import mongo4s.bson.BsonInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given
import medeiaCodecs.given

object CoreKyoMedeiaApp extends KyoApp:

  run:
    Scope.run:
      for
        client <- MongoClientResource.fromConnectionString("mongodb://localhost:27018")
        db     <- client.getDatabase("mongo4s_examples")
        users  <- db.getCollection[User]("core_kyo_medeia_users")
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
