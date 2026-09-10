package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO

import mongo4s.bson.FieldNaming
import mongo4s.bson.direct.WireCodec
import mongo4s.cats.CatsStream
import mongo4s.repositories.BaseMongoRepository
import mongo4s.{MongoClient, PrimaryKey}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object KeysetPageItSpec:
  final case class Reading(sensor: String, seq: Int, value: Double) derives WireCodec

  type Key = (sensor: String, seq: Int)

  object Reading:
    given PrimaryKey[Reading, Key] = PrimaryKey.compound(r => (sensor = r.sensor, seq = r.seq))

final class KeysetPageItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import KeysetPageItSpec.{Key, Reading}
  import KeysetPageItSpec.Reading.given

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val readings =
    List(
      Reading("a", 1, 1.0),
      Reading("a", 2, 2.0),
      Reading("b", 1, 3.0),
      Reading("b", 2, 4.0),
      Reading("c", 1, 5.0),
    )

  private def repository(name: String): IO[BaseMongoRepository[IO, S, Reading, Key]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("keyset_it")
      collection <- database.getDirectCollection[Reading](name)
      repo        = BaseMongoRepository[IO, S, Reading, Key](collection)
      _          <- repo.ensureKeyIndex
      _          <- repo.insertMany(scala.util.Random.shuffle(readings))
    yield repo

  private def walk(repo: BaseMongoRepository[IO, S, Reading, Key], size: Int): IO[List[List[Reading]]] =
    def go(after: Option[Key], acc: List[List[Reading]]): IO[List[List[Reading]]] =
      repo.findPage(size, after).flatMap { page =>
        if page.isEmpty
        then IO.pure(acc.reverse)
        else go(Some((sensor = page.last.sensor, seq = page.last.seq)), page :: acc)
      }

    go(None, Nil)

  "findPage" should {

    "walk a compound key in order, without repeating or skipping a row" in {
      val program = repository("compound").flatMap(walk(_, 2))

      program.timeout(60.seconds).asserting { pages =>
        pages.map(_.size) shouldBe List(2, 2, 1)
        pages.flatten shouldBe readings
      }
    }

    "return the same rows whatever the page size" in {
      val program =
        for
          repo   <- repository("sizes")
          byOne  <- walk(repo, 1)
          byFour <- walk(repo, 4)
        yield (byOne.flatten, byFour.flatten)

      program.timeout(60.seconds).asserting { (byOne, byFour) =>
        byOne shouldBe readings
        byFour shouldBe readings
      }
    }

    "stay correct when rows before the cursor are deleted mid-walk" in {
      val program =
        for
          repo  <- repository("shifting")
          first <- repo.findPage(2)
          _     <- repo.deleteOne((sensor = "a", seq = 1))
          next  <- repo.findPage(2, after = Some((sensor = first.last.sensor, seq = first.last.seq)))
        yield next

      program.timeout(60.seconds).asserting(_ shouldBe List(Reading("b", 1, 3.0), Reading("b", 2, 4.0)))
    }
  }
