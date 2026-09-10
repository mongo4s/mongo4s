package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO

import mongo4s.bson.FieldNaming
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

object SelectItSpec:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  final case class Person(id: String, fullName: String, age: Int, secret: String) derives WireCodec

  final case class Contact(id: String, nick: Option[String]) derives WireCodec

final class SelectItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import SelectItSpec.Person

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def seeded(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("select_it")
      collection <- database.getDirectCollection[Person](name, FieldNaming.snakeCase)
      _          <- collection.insertMany(List(Person("1", "bob", 30, "hidden"), Person("2", "alice", 25, "hidden")))
    yield collection

  "selectAs on a direct collection" should {

    "read a projection that drops a field the entity declares" in {
      val program =
        for
          collection <- seeded("people")
          picked     <- collection.find().selectAs[(fullName: String, age: Int)].all
        yield picked

      program.timeout(30.seconds).asserting { picked =>
        picked.map(_.fullName) should contain theSameElementsAs List("bob", "alice")
        picked.map(_.age) should contain theSameElementsAs List(30, 25)
      }
    }

    "spell the projected names the way the collection stores them" in {
      val program =
        for
          collection <- seeded("naming")
          picked     <- collection.find().selectAs[(fullName: String)].first
        yield picked

      program.timeout(30.seconds).asserting(_.map(_.fullName) shouldBe Some("bob"))
    }

    "keep the query it was built from" in {
      val program =
        for
          collection <- seeded("filtered")
          picked     <- collection
                          .find(Field.of[Person, Int](_.age).lt(28))
                          .sort(mongo4s.operations.Sort.asc(Field.of[Person, String](_.fullName)))
                          .selectAs[(fullName: String, age: Int)]
                          .all
        yield picked

      program.timeout(30.seconds).asserting { picked =>
        picked.map(_.fullName) shouldBe List("alice")
      }
    }

    "leave the entity codec as strict as it was, which is the gap it closes" in {
      val program =
        for
          collection <- seeded("strict")
          outcome    <- collection
                          .find()
                          .projection(mongo4s.operations.Projection.empty[Person].include(Field.of[Person, Int](_.age)))
                          .all
                          .attempt
        yield outcome

      program.timeout(30.seconds).asserting(_.isLeft shouldBe true)
    }
  }

  "selectAs over an optional field" should {

    "give the same answer as reading the whole entity, when the field was never written" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("select_it")
          collection <- database.getDirectCollection[SelectItSpec.Contact]("contacts")
          _          <- collection.insertMany(List(SelectItSpec.Contact("1", None), SelectItSpec.Contact("2", Some("bo"))))
          whole      <- collection.find().all.map(_.map(_.nick))
          selected   <- collection.find().selectAs[(id: String, nick: Option[String])].all.map(_.map(_.nick))
        yield (whole, selected)

      program.timeout(30.seconds).asserting { (whole, selected) =>
        whole shouldBe List(None, Some("bo"))
        selected shouldBe whole
      }
    }
  }
