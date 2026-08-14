package mongo4s.examples

import java.time.Instant

import cats.effect.{IO, IOApp, Resource}
import org.bson.types.ObjectId

import mongo4s.cats.CatsStream
import mongo4s.bson.BsonDocumentCodec
import mongo4s.{MongoClient, PrimaryKey, WithId}
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}
import mongo4s.repositories.BaseMongoRepository

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

import bsonDirectCodecs.given

final case class ApiToken(_id: ObjectId, owner: UserId, label: String, issuedAt: Instant) derives WireCodec

object ApiToken:
  given PrimaryKey[ApiToken, ObjectId] = PrimaryKey.storedId(_._id)
  given BsonDocumentCodec[ApiToken]    = DocumentCodecBridge.toDocumentCodec[ApiToken]

final case class LoginEvent(userId: UserId, at: Instant) derives WireCodec

object LoginEvent:
  given BsonDocumentCodec[LoginEvent] = DocumentCodecBridge.toDocumentCodec[LoginEvent]

object RepositoryCatsBsonDirectApp extends IOApp.Simple:

  type S[A] = CatsStream[IO][A]

  private def mongoClient(connectionString: String): Resource[IO, MongoClient[IO, S]] =
    Resource.make(MongoClient.fromConnectionString[IO, S](connectionString))(_.close)

  def run: IO[Unit] =
    mongoClient("mongodb://localhost:27018").use: client =>
      for
        db <- client.getDatabase("mongo4s_examples")

        // 1. regular model
        userCollection <- db.getDirectCollection[User]("repo_direct_users")
        users           = new BaseMongoRepository(userCollection)
        alice           = User(
                            UserId("1"),
                            "Alice",
                            "alice@example.com",
                            30,
                            Role.Admin,
                            Address("New York", "10001"),
                            List("vip"),
                            active = true,
                            Instant.now(),
                          )
        _              <- users.insertOne(alice)
        foundUser      <- users.findOne(UserId("1"))
        _              <- IO.println(s"user: $foundUser")

        // 2. id: ObjectId
        tokens     <- BaseMongoRepository.identified[IO, S, ApiToken, ObjectId](db, "repo_direct_tokens")
        token       = ApiToken(ObjectId.get(), UserId("1"), "cli-token", Instant.now())
        _          <- tokens.insertOne(token)
        foundToken <- tokens.findOne(token._id)
        _          <- IO.println(s"token: $foundToken")

        // 3. WithId
        events     <- BaseMongoRepository.objectId[IO, S, LoginEvent](db, "repo_direct_events")
        eventId     = ObjectId.get()
        _          <- events.insertOne(WithId(eventId, LoginEvent(UserId("1"), Instant.now())))
        foundEvent <- events.findOne(eventId)
        _          <- IO.println(s"event: $foundEvent")
      yield ()
