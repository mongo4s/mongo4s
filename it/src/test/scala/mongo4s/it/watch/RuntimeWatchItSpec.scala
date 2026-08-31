package mongo4s.it.watch

import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import com.mongodb.client.model.changestream.OperationType

import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.{Effect, Field, MongoClient, RsBridge, Streamable}

import mongo4s.bson.BsonInstances.given

trait RuntimeWatchItSpec[F[*], S[*]] extends AnyWordSpec, Matchers:
  import WatchFixture.Person
  import WatchFixture.Person.given

  protected def runtimeName: String

  protected def effectInstance: Effect[F]
  protected def rsBridge: RsBridge[F, S]
  protected def streamable: Streamable[S, ChangeEvent[Person]]

  protected def run[A](fa: F[A]): A
  protected def takeEvents(stream: S[ChangeEvent[Person]], n: Int): List[ChangeEvent[Person]]

  private def collecting(n: Int)(stream: => S[ChangeEvent[Person]])(writes: => Unit): List[ChangeEvent[Person]] =
    val result = CompletableFuture[List[ChangeEvent[Person]]]()

    val worker = Thread { () =>
      try result.complete(takeEvents(stream, n)): Unit
      catch case error: Throwable => result.completeExceptionally(error): Unit
    }

    worker.setDaemon(true)
    worker.start()

    writes

    result.get(40, TimeUnit.SECONDS)
  end collecting

  s"$runtimeName change streams" should {

    "deliver events without waiting for the 256-element buffer to fill" in {
      given Effect[F]                          = effectInstance
      given RsBridge[F, S]                     = rsBridge
      given Streamable[S, ChangeEvent[Person]] = streamable

      val client     = run(MongoClient.fromConnectionString[F, S](WatchFixture.connectionString))
      val database   = run(client.getDatabase(s"runtime_watch_${runtimeName.toLowerCase}"))
      val collection = run(database.getCollection[Person]("events"))
      val now        = run(database.runCommand(WatchFixture.Hello)).getTimestamp("operationTime")

      val events = collecting(2)(collection.watch(WatchOptions.default[Person].startingAt(now))) {
        run(collection.insertOne(Person("bob", 30)))
        run(collection.updateOne(Field.of[Person, String](_.name).equalTo("bob"), Field.of[Person, Int](_.age).set(31)))
        run(collection.insertOne(Person("carol", 41)))
      }

      run(client.close)

      events.map(_.operationType) shouldBe List(OperationType.INSERT, OperationType.UPDATE)
      events.flatMap(_.fullDocument) shouldBe List(Person("bob", 30), Person("bob", 31))
    }
  }
