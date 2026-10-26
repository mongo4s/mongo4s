package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import cats.syntax.all.*
import org.bson.{BsonDocument, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.bson.medeia.MedeiaDocumentCodec
import mongo4s.bson.direct.{WireCodec, WireCodecConfig}
import mongo4s.{Field, MongoClient, MongoDatabase, PrimaryKey, RsBridge}
import mongo4s.repositories.{BaseMongoRepository, Counter, MongoCounters}

import scala.concurrent.duration.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given

object ConditionalWritesItSpec:

  final case class Person(id: String, name: String, version: Int) derives MedeiaDocumentCodec

  object Person:
    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

  final case class Seat(_id: String, row: Int, holder: String) derives MedeiaDocumentCodec

  object Seat:
    given PrimaryKey[Seat, String] = PrimaryKey.storedId(_._id)

  final case class Booking(hall: String, seat: Int, holder: String) derives MedeiaDocumentCodec

  object Booking:
    given PrimaryKey[Booking, (hall: String, seat: Int)] = PrimaryKey.compound(booking => (hall = booking.hall, seat = booking.seat))

  object Lenient:
    given WireCodecConfig = WireCodecConfig.Default.withDecodeMissingAsDefault(true)
    final case class Account(name: String, status: String = "active", since: Option[Int] = Some(2020)) derives WireCodec

final class ConditionalWritesItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ConditionalWritesItSpec.*

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private def withDatabase[A](name: String)(use: (MongoClient[IO, S], MongoDatabase[IO, S]) => IO[A]): IO[A] =
    MongoClient
      .fromConnectionString[IO, S](container.getConnectionString)
      .flatMap(client => client.getDatabase(name).flatMap(use(client, _)).guarantee(client.close))
      .timeout(30.seconds)

  "insertIfAbsent against a real MongoDB" should {

    "insert a new key, and leave an existing one alone, on a key that is not _id" in {
      withDatabase("conditional_insert_field") { (_, database) =>
        for
          repo   <- BaseMongoRepository.create[IO, S, Person, String](database, "people")
          first  <- repo.insertIfAbsent(Person("1", "bob", 1))
          second <- repo.insertIfAbsent(Person("1", "eve", 9))
          stored <- repo.findByFilter(mongo4s.operations.Filter.all)
        yield (first, second, stored)
      }.asserting(_ shouldBe (true, false, List(Person("1", "bob", 1))))
    }

    "do the same on an _id key" in {
      withDatabase("conditional_insert_id") { (_, database) =>
        for
          repo   <- BaseMongoRepository.create[IO, S, Seat, String](database, "seats")
          first  <- repo.insertIfAbsent(Seat("a1", 1, "bob"))
          second <- repo.insertIfAbsent(Seat("a1", 1, "eve"))
          stored <- repo.findOne("a1")
        yield (first, second, stored)
      }.asserting(_ shouldBe (true, false, Some(Seat("a1", 1, "bob"))))
    }

    "do the same on a compound key" in {
      withDatabase("conditional_insert_compound") { (_, database) =>
        for
          repo   <- BaseMongoRepository.create[IO, S, Booking, (hall: String, seat: Int)](database, "bookings")
          first  <- repo.insertIfAbsent(Booking("red", 7, "bob"))
          other  <- repo.insertIfAbsent(Booking("red", 8, "eve"))
          again  <- repo.insertIfAbsent(Booking("red", 7, "eve"))
          stored <- repo.count()
        yield (first, other, again, stored)
      }.asserting(_ shouldBe (true, true, false, 2L))
    }

    "answer false inside a transaction without spoiling it" in {
      withDatabase("conditional_insert_tx") { (client, database) =>
        for
          repo    <- BaseMongoRepository.create[IO, S, Person, String](database, "people")
          _       <- repo.insertOne(Person("1", "bob", 1))
          answers <- client.withTransaction((repo.insertIfAbsent(Person("1", "eve", 9)), repo.insertIfAbsent(Person("2", "ann", 1))).tupled)
          stored  <- repo.count()
        yield (answers, stored)
      }.asserting(_ shouldBe ((false, true), 2L))
    }
  }

  "replace against a real MongoDB" should {

    "replace only while the condition holds and never insert" in {
      val version = Field.of[Person, Int](_.version)

      withDatabase("conditional_replace") { (_, database) =>
        for
          repo    <- BaseMongoRepository.create[IO, S, Person, String](database, "people")
          _       <- repo.insertOne(Person("1", "bob", 1))
          won     <- repo.replace(Person("1", "first", 2), version.equalTo(1))
          lost    <- repo.replace(Person("1", "second", 2), version.equalTo(1))
          missing <- repo.replace(Person("2", "nobody", 1))
          stored  <- repo.findByFilter(mongo4s.operations.Filter.all)
        yield (won.matchedCount, lost.matchedCount, missing.wasApplied, stored)
      }.asserting(_ shouldBe (1L, 0L, false, List(Person("1", "first", 2))))
    }
  }

  "MongoCounters against a real MongoDB" should {

    "count up from one, per name, by a step" in {
      withDatabase("counters_sequence") { (_, database) =>
        for
          counters <- MongoCounters.create[IO, S](database)
          first    <- counters.next("orders")
          second   <- counters.next("orders")
          other    <- counters.next("invoices")
          block    <- counters.next("orders", step = 10L)
          current  <- counters.current("orders")
          never    <- counters.current("never")
        yield (first, second, other, block, current, never)
      }.asserting(_ shouldBe (1L, 2L, 1L, 12L, Some(12L), None))
    }

    "hand out every value exactly once under concurrent callers" in {
      withDatabase("counters_concurrent") { (_, database) =>
        for
          counters <- MongoCounters.create[IO, S](database, "sequences")
          values   <- List.fill(50)(counters.next("orders")).parSequence
        yield values.sorted
      }.asserting(_ shouldBe (1L to 50L).toList)
    }

    "carry on from a reset, and store one document per name under _id" in {
      withDatabase("counters_reset") { (_, database) =>
        for
          counters <- MongoCounters.create[IO, S](database)
          _        <- counters.reset("orders", 41L)
          next     <- counters.next("orders")
          stored   <- database.getCollection[Counter](MongoCounters.DefaultCollection).flatMap(_.find().all)
          raw      <- database.getCollection[BsonDocument](MongoCounters.DefaultCollection).flatMap(_.find().all)
        yield (next, stored, raw.map(_.get("_id")))
      }.asserting(_ shouldBe (42L, List(Counter("orders", 42L)), List(BsonString("orders"))))
    }
  }

  "decodeMissingAsDefault against a real MongoDB" should {

    "read a document written before a field existed with the field's default" in {
      withDatabase("missing_as_default") { (_, database) =>
        for
          collection <- database.getDirectCollection[Lenient.Account]("accounts")
          _          <- RsBridge[IO, S].one(collection.underlying.insertOne(BsonDocument("name", BsonString("ann"))))
          read       <- collection.find().all
        yield read
      }.asserting(_ shouldBe List(Lenient.Account("ann", "active", Some(2020))))
    }
  }
