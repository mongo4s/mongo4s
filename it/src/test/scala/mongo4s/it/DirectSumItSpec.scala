package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO

import mongo4s.MongoClient
import mongo4s.cats.CatsStream
import mongo4s.operations.Filter
import mongo4s.bson.direct.WireCodec

import mongo4s.cats.CatsInstances.given

object DirectSumItSpec:

  enum Shape derives WireCodec:
    case Circle(radius: Double)
    case Square(side: Double)

  final case class Draft(title: String) derives WireCodec
  final case class Published(title: String, slug: String) derives WireCodec

final class DirectSumItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import DirectSumItSpec.{Draft, Published, Shape}

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  "a sealed hierarchy stored as the root entity of a direct collection" should {
    "read back after the server has stored _id ahead of the discriminator" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("direct_sum_it")
          collection <- database.getDirectCollection[Shape]("shapes")
          _          <- collection.insertOne(Shape.Circle(2.0))
          found      <- collection.find(Filter.all).all
        yield found

      program.asserting(_ shouldBe List(Shape.Circle(2.0)))
    }

    "dispatch every stored subtype through its own discriminator" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("direct_sum_it")
          collection <- database.getDirectCollection[Shape]("mixed")
          _          <- collection.insertMany(List(Shape.Circle(1.0), Shape.Square(3.0), Shape.Circle(4.0)))
          found      <- collection.find(Filter.all).all
        yield found

      program.asserting(_ shouldBe List(Shape.Circle(1.0), Shape.Square(3.0), Shape.Circle(4.0)))
    }

    "read back an Either stored as the root entity, which has its own discriminator reader" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("direct_sum_it")
          collection <- database.getDirectCollection[Either[Draft, Published]]("posts")
          _          <- collection.insertOne(Left(Draft("wip")))
          found      <- collection.find(Filter.all).all
        yield found

      program.asserting(_ shouldBe List(Left(Draft("wip"))))
    }
  }
