package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer
import com.github.dockerjava.api.model.{ExposedPort, PortBinding, Ports}

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}
import com.mongodb.client.model.changestream.OperationType

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class WatchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person

  private val container = new MongoDBContainer("mongo:7")
    .withCreateContainerCmdModifier(cmd => cmd.getHostConfig.withPortBindings(PortBinding(Ports.Binding.bindPort(27017), ExposedPort(27017))))

  override def beforeAll(): Unit =
    container.start()
    container.execInContainer(
      "mongosh",
      "--quiet",
      "--eval",
      "rs.reconfig(Object.assign(rs.conf(), {members: [{_id: 0, host: 'localhost:27017'}]}))",
    )

  override def afterAll(): Unit = container.stop()

  type S[A] = CatsStream[IO][A]

  "MongoCollection.watch" should {

    "report operationType and, thanks to UPDATE_LOOKUP, the current fullDocument on inserts and updates" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people")
          fiber      <- collection.watch().take(3).compile.toList.start
          _          <- IO.sleep(500.millis) // let the change stream cursor open before writing
          _          <- collection.insertOne(Person("bob", 30))
          _          <- collection.updateOne(Field.of[Person, String](_.name).equalTo("bob"), Field.of[Person, Int](_.age).set(31))
          _          <- collection.deleteOne(Field.of[Person, String](_.name).equalTo("bob"))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE)
        events(0).fullDocument shouldBe Some(Person("bob", 30))
        events(1).fullDocument shouldBe Some(Person("bob", 31))
        events(2).fullDocument shouldBe None
      }
    }

    "filter events through a raw pipeline matching against the change-event envelope" in {
      val insertOnly = Seq(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))

      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("test")
          collection <- database.getCollection[Person]("watch_people_filtered")
          fiber      <- collection.watch(pipeline = insertOnly).take(1).compile.toList.start
          _          <- IO.sleep(500.millis)
          _          <- collection.insertOne(Person("alice", 25))
          _          <- collection.updateOne(Field.of[Person, String](_.name).equalTo("alice"), Field.of[Person, Int](_.age).set(26))
          events     <- fiber.joinWithNever
          _          <- client.close
        yield events

      program.timeout(30.seconds).asserting { events =>
        events.map(_.operationType) shouldBe List(OperationType.INSERT)
      }
    }
  }
