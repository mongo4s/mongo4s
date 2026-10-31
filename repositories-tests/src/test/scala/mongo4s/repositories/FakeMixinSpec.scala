package mongo4s.repositories

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.{Field, PrimaryKey}
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.testkit.{FakeMongoCollection, FakeRepository}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object FakeMixinSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Player(playerId: String, fullName: String, age: Int) derives WireCodec

  object Player:
    given PrimaryKey[Player, String] = PrimaryKey.of(_.playerId)

  trait PlayerRepository[F[*]] extends Repository[F, CatsStream[F], Player, String]:
    def adults: F[List[Player]]

final class FakeMixinSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import FakeMixinSpec.{Player, PlayerRepository}

  "a FakeRepository over a WireCodec" should {

    "take the codec's naming, and mix into a service's own repository trait" in {
      val fake = FakeMongoCollection.direct[IO, CatsStream[IO], Player]()

      val players: PlayerRepository[IO] =
        new FakeRepository[IO, CatsStream[IO], Player, String](fake) with PlayerRepository[IO]:
          def adults: IO[List[Player]] = findByFilter(Field.of[Player](_.age).gte(18))

      for
        _      <- players.insertMany(List(Player("1", "Ann", 30), Player("2", "Bo", 12)))
        adults <- players.adults
        found  <- players.findOne("2")
      yield
        adults shouldBe List(Player("1", "Ann", 30))
        found shouldBe Some(Player("2", "Bo", 12))
        fake.naming("fullName") shouldBe "full_name"
        fake.snapshot.map(_.fullName) should contain theSameElementsAs List("Ann", "Bo")
    }

    "be built in one call from the codec and the key" in {
      val repository = FakeRepository.direct[IO, CatsStream[IO], Player, String]()

      repository.insertOne(Player("1", "Ann", 30)) *> repository.findByField(Field.of[Player](_.fullName), "Ann").asserting(_ shouldBe List(Player("1", "Ann", 30)))
    }
  }
