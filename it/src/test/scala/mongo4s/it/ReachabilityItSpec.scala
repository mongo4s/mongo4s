package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO

import mongo4s.MongoClient
import mongo4s.cats.{CatsStream, MongoClientResource}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

final class ReachabilityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  "ping" should {

    "answer true for a server that answers" in {
      MongoClientResource.fromConnectionString[IO](container.getConnectionString).use(_.ping(5.seconds)).asserting(_ shouldBe true)
    }

    "answer false within its timeout for one that does not, rather than failing" in {
      val program =
        MongoClientResource.fromConnectionString[IO]("mongodb://localhost:1/?directConnection=true").use { client =>
          client.ping(300.millis).timed
        }

      program.timeout(10.seconds).asserting { (took, answer) =>
        answer shouldBe false
        took should be < 5.seconds
      }
    }
  }

  "reachability" should {

    "report the server reachable once it has pinged it" in {
      MongoClientResource
        .fromConnectionString[IO](container.getConnectionString)
        .flatMap(client => MongoClientResource.reachability[IO, S](client, 1.second))
        .use(identity)
        .asserting(_ shouldBe true)
    }

    "report it unreachable when the ping fails" in {
      val unreachable: IO[MongoClient[IO, S]] = MongoClient.fromConnectionString[IO, S]("mongodb://localhost:1/?directConnection=true")

      cats.effect.Resource
        .make(unreachable)(_.close)
        .flatMap(client => MongoClientResource.reachability[IO, S](client, 300.millis))
        .use(identity)
        .timeout(10.seconds)
        .asserting(_ shouldBe false)
    }
  }
