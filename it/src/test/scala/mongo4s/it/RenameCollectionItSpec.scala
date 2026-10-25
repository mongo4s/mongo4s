package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.{MongoClient, MongoDatabase, MongoError}

import mongo4s.cats.CatsInstances.given

final class RenameCollectionItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def database: IO[MongoDatabase[IO, S]] =
    MongoClient.fromConnectionString[IO, S](container.getConnectionString).flatMap(_.getDatabase("rename_it"))

  private def codeOf(outcome: Either[Throwable, Unit]): Option[Int] =
    outcome.left.toOption.collect { case failed: MongoError => failed.code }

  "renameCollection" should {

    "move the documents under the new name and leave the old name empty" in {
      val program =
        for
          db     <- database
          source <- db.getCollection[Person]("moved_from")
          _      <- source.insertMany(List(Person("bob", 30), Person("ann", 25)))
          _      <- source.renameCollection("moved_to")
          target <- db.getCollection[Person]("moved_to")
          moved  <- target.find().all
          left   <- source.count()
        yield (moved.map(_.name).sorted, left)

      program.asserting(_ shouldBe (List("ann", "bob"), 0L))
    }

    "refuse to replace a collection that already exists" in {
      val program =
        for
          db     <- database
          source <- db.getCollection[Person]("clash_from")
          target <- db.getCollection[Person]("clash_to")
          _      <- source.insertOne(Person("bob", 30))
          _      <- target.insertOne(Person("ann", 25))
          failed <- source.renameCollection("clash_to").attempt
          kept   <- target.find().all
        yield (codeOf(failed), kept.map(_.name))

      program.asserting(_ shouldBe (Some(48), List("ann")))
    }

    "replace the existing collection when told to drop it" in {
      val program =
        for
          db       <- database
          source   <- db.getCollection[Person]("replace_from")
          target   <- db.getCollection[Person]("replace_to")
          _        <- source.insertOne(Person("bob", 30))
          _        <- target.insertOne(Person("ann", 25))
          _        <- source.renameCollection("replace_to", dropTarget = true)
          replaced <- target.find().all
        yield replaced.map(_.name)

      program.asserting(_ shouldBe List("bob"))
    }

    "fail for a collection that does not exist" in {
      val program =
        for
          db      <- database
          missing <- db.getCollection[Person]("never_created")
          failed  <- missing.renameCollection("anything").attempt
        yield codeOf(failed)

      program.asserting(_ shouldBe Some(26))
    }

    "refuse a name no collection can have, before the server is asked" in {
      val program =
        for
          db     <- database
          source <- db.getCollection[Person]("badly_named")
          _      <- source.insertOne(Person("bob", 30))
          failed <- source.renameCollection("").attempt
          left   <- source.count()
        yield (failed.left.toOption.map(_.getClass.getSimpleName), left)

      program.asserting(_ shouldBe (Some("IllegalArgumentException"), 1L))
    }
  }
