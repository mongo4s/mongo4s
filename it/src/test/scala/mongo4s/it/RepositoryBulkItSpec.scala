package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.BsonDocument

import mongo4s.bson.direct.WireCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.{Index, WriteCommand}
import mongo4s.repositories.BaseMongoRepository
import mongo4s.{Field, MongoClient, PrimaryKey}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object RepositoryBulkItSpec:

  final case class Person(id: String, email: String) derives WireCodec

  object Person:
    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

final class RepositoryBulkItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import RepositoryBulkItSpec.Person
  import RepositoryBulkItSpec.Person.given

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val email = Field.of[Person, String](_.email)

  private def repository(name: String): IO[BaseMongoRepository[IO, S, Person, String]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("repository_bulk_it")
      collection <- database.getDirectCollection[Person](name)
      _          <- collection.createIndex(Index.ascending(email).withUnique)
    yield BaseMongoRepository(collection, batchSize = 2)

  private def inserts(ids: String*): Seq[WriteCommand[Person]] =
    ids.map(id => WriteCommand.InsertOne(Person(id, s"$id@x")))

  "an unordered bulk write" should {

    "apply every command the server could apply, not stop at the first failing batch" in {
      val program =
        for
          repo    <- repository("unordered")
          _       <- repo.insertOne(Person("dup", "clash@x"))
          failed  <- repo
                       .bulkWrite(
                         Seq(WriteCommand.InsertOne(Person("a", "clash@x"))) ++ inserts("b", "c", "d"),
                         ordered = false,
                       )
                       .attempt
          present <- repo.count()
        yield (failed.isLeft, present)

      program.timeout(30.seconds).asserting { result =>
        val (failed, present) = result
        failed shouldBe true
        present shouldBe 4L
      }
    }
  }

  "an ordered bulk write" should {

    "stop at the first failure, leaving what came before it applied" in {
      val program =
        for
          repo    <- repository("ordered")
          _       <- repo.insertOne(Person("dup", "clash@x"))
          failed  <- repo
                       .bulkWrite(inserts("a", "b") ++ Seq(WriteCommand.InsertOne(Person("c", "clash@x"))) ++ inserts("d"))
                       .attempt
          present <- repo.count()
        yield (failed.isLeft, present)

      program.timeout(30.seconds).asserting { result =>
        val (failed, present) = result
        failed shouldBe true
        present shouldBe 3L
      }
    }
  }
