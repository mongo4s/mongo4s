package mongo4s.cats

import scala.util.Try

import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec

import cats.effect.{IO, Resource}
import org.bson.BsonDocument

import mongo4s.bson.direct.WireCodec
import mongo4s.operations.{CreateCollectionOptions, Stage}
import mongo4s.operations.geometry.{GeoShape, Geometry}
import mongo4s.{Field, MongoClient, MongoCollection, MongoDatabase}

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object DeferredFailureSpec:
  final case class Place(name: String, location: List[Double]) derives WireCodec

final class DeferredFailureSpec extends AsyncWordSpec, AsyncIOSpec, Matchers:
  import DeferredFailureSpec.Place

  type S[A] = CatsStream[IO][A]

  private val unreachable = "mongodb://localhost:1/?serverSelectionTimeoutMS=200"

  private val location = Field.stored[BsonDocument, BsonDocument]("location")
  private val name     = Field.stored[BsonDocument, String]("name")
  private val placeAt  = Field.of[Place](_.location)

  private val open = GeoShape.Within(Geometry.Polygon(List(Geometry.Point(0, 0), Geometry.Point(1, 0), Geometry.Point(1, 1))))

  private def database: Resource[IO, MongoDatabase[IO, S]] =
    Resource.make(MongoClient.fromConnectionString[IO, S](unreachable))(_.close).evalMap(_.getDatabase("deferred"))

  private def documents: Resource[IO, MongoCollection[IO, S, BsonDocument]] = database.evalMap(_.getCollection[BsonDocument]("places"))

  private def places: Resource[IO, MongoCollection[IO, S, Place]] = database.evalMap(_.getDirectCollection[Place]("places"))

  private def outcome[A](build: => IO[A]): IO[Either[String, Either[Throwable, A]]] =
    IO(Try(build).toEither).flatMap {
      case Left(thrown) => IO.pure(Left(s"thrown while the operation was built: $thrown"))
      case Right(io)    => io.attempt.map(Right(_))
    }

  private def failsInside[A](build: => IO[A]): IO[Unit] =
    outcome(build).map {
      case Right(Left(_: IllegalArgumentException)) => ()
      case other                                    => fail(s"expected an IllegalArgumentException raised in IO, got $other")
    }

  "an operation that renders or validates its arguments" should {
    "raise the failure inside the effect, where attempt sees it" in {
      documents.use { collection =>
        for
          _ <- failsInside(collection.aggregate[BsonDocument](Seq(Stage.matching(location.within(open)))).all)
          _ <- failsInside(collection.aggregate[BsonDocument](Seq(Stage.matching(location.within(open)))).first)
          _ <- failsInside(collection.distinct(name, location.within(open)).all)
        yield succeed
      }
    }

    "do the same on a direct collection" in {
      places.use { collection =>
        for
          _ <- failsInside(collection.aggregate[BsonDocument](Seq(Stage.matching(placeAt.within(open)))).all)
          _ <- failsInside(collection.aggregateDirect[Place](Seq(Stage.matching(placeAt.within(open)))).all)
          _ <- failsInside(collection.distinct(Field.of[Place](_.name), placeAt.within(open)).all)
        yield succeed
      }
    }

    "refuse create options that contradict each other when the effect runs" in {
      database.use(db => failsInside(db.createCollection("capped", CreateCollectionOptions.default.withMaxDocuments(5))).as(succeed))
    }

    "leave the driver's own argument checks to the effect as well" in {
      documents.use { collection =>
        for
          _ <- failsInside(collection.dropIndex(null))
          _ <- database.use(db => failsInside(db.dropCollection(null)))
          _ <- database.use(db => failsInside(db.runCommand(null)))
        yield succeed
      }
    }
  }
