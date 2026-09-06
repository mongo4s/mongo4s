package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.Stage
import mongo4s.testkit.FakeMongoCollection

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

final class ExplainSpec extends AnyWordSpec, Matchers:
  import AggregateSpec.Person
  import AggregateSpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def collection: FakeMongoCollection[IO, S, Person] =
    val fake = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]], _ => fs2.Stream.empty)
    fake.insertMany(AggregateSpec.people).unsafeRunSync()
    fake

  "the fake" should {

    "refuse to explain a find, rather than invent a query plan" in {
      val refused = intercept[UnsupportedOperationException](collection.find().explain())

      refused.getMessage should include("explain")
    }

    "refuse to explain an aggregation for the same reason" in {
      val refused = intercept[UnsupportedOperationException] {
        collection.aggregate[Person](Seq(Stage.limit(1))).explain()
      }

      refused.getMessage should include("explain")
    }
  }
