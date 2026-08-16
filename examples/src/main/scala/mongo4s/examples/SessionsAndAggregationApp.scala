package mongo4s.examples

import java.time.Instant

import cats.effect.{IO, IOApp}

import mongo4s.Field
import mongo4s.cats.{CatsStream, MongoClientResource}
import mongo4s.operations.{Sort, Stage}

import mongo4s.bson.BsonInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given
import mongo4s.cats.CatsInstances.given
import medeiaCodecs.given

object SessionsAndAggregationApp extends IOApp.Simple:

  type S[A] = CatsStream[IO][A]

  def run: IO[Unit] =
    MongoClientResource.fromConnectionString[IO]("mongodb://localhost:27018").use: client =>
      for
        db          <- client.getDatabase("mongo4s_examples")
        users       <- db.getCollection[User]("sessions_and_aggregation_users")

        // withTransaction starts a session, gives it implicitly to everything inside the body, and
        // commits/aborts + closes the session for you — the insert stays invisible outside the
        // session until it commits
        outside     <- users.count()
        inSession   <- client.withTransaction {
                         users.insertOne(sampleUser("2", "Bob", 41)) *> users.count()
                       }
        afterCommit <- users.count()
        _           <- IO.println(s"count inside transaction: $inSession, outside before commit: $outside, after commit: $afterCommit")

        // an aggregation pipeline built from typed Stage values instead of raw BsonDocument
        adults      <- users
                         .aggregate[User](
                           Seq(
                             Stage.matching(Field.of[User, Int](_.age).gte(18)),
                             Stage.sortBy(Sort.asc(Field.of[User, String](_.name))),
                           )
                         )
                         .all
        _           <- IO.println(s"adults, sorted by name: ${adults.map(_.name)}")
      yield ()

  private def sampleUser(id: String, name: String, age: Int): User =
    User(
      UserId(id),
      name,
      s"${name.toLowerCase}@example.com",
      age,
      Role.Member,
      Address("Berlin", "10115"),
      Nil,
      active = true,
      Instant.now(),
    )
