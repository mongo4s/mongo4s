package mongo4s.it

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.types.ObjectId

import mongo4s.cats.CatsStream
import mongo4s.{MongoClient, PrimaryKey}
import mongo4s.bson.BsonDocumentCodec
import mongo4s.repositories.BaseMongoRepository
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object WithoutIdItSpec:

  final case class ApiToken(_id: ObjectId, label: String, issuedAt: Instant) derives WireCodec

  object ApiToken:
    given PrimaryKey[ApiToken, ObjectId] = PrimaryKey.storedId(_._id)
    given BsonDocumentCodec[ApiToken]    = DocumentCodecBridge.toDocumentCodec[ApiToken]

final class WithoutIdItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import WithoutIdItSpec.ApiToken
  import WithoutIdItSpec.ApiToken.given

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def database = MongoClient.fromConnectionString[IO, S](container.getConnectionString).flatMap(_.getDatabase("without_id_it"))

  "BaseMongoRepository.withoutId on an entity that models _id" should {
    "fail to read back, which is why the docs restrict it to entities that do not model _id" in {
      val token = ApiToken(ObjectId.get(), "cli", Instant.now().truncatedTo(ChronoUnit.MILLIS))

      val program =
        for
          db     <- database
          tokens <- BaseMongoRepository.withoutId[IO, S, ApiToken, ObjectId](db, "stripped")
          _      <- tokens.insertOne(token)
          found  <- tokens.findOne(token._id)
        yield found

      program.attempt.asserting(_.isLeft shouldBe true)
    }

    "read back through create, which keeps _id in the projection" in {
      val token = ApiToken(ObjectId.get(), "cli", Instant.now().truncatedTo(ChronoUnit.MILLIS))

      val program =
        for
          db     <- database
          tokens <- BaseMongoRepository.create[IO, S, ApiToken, ObjectId](db, "kept")
          _      <- tokens.insertOne(token)
          found  <- tokens.findOne(token._id)
        yield found

      program.asserting(_ shouldBe Some(token))
    }
  }
