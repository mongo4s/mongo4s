package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import java.util.concurrent.TimeUnit

import cats.effect.IO
import com.mongodb.{ConnectionString, MongoClientSettings, WriteConcern}
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.operations.{Filter, Index}
import mongo4s.{Field, MongoClient, MongoCollection, MongoError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given

final class MongoErrorItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val email = Field.stored[BsonDocument, String]("email")

  private def person(address: String): BsonDocument = BsonDocument("email", BsonString(address))

  private def unique(name: String): IO[MongoCollection[IO, S, BsonDocument]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("mongo_error_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.createIndex(Index.ascending(email).withUnique)
    yield collection

  "a unique index violation" should {

    "arrive as MongoError.DuplicateKey from insertOne" in {
      val program =
        for
          collection <- unique("insert_one")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.insertOne(person("a@b.c")).attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(error: MongoError.DuplicateKey) => error.code shouldBe 11000
        case other                                => fail(s"expected a DuplicateKey, got $other")
      }
    }

    "arrive as MongoError.BulkWriteFailed from insertMany, naming which document failed" in {
      val program =
        for
          collection <- unique("insert_many")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.insertMany(List(person("x@y.z"), person("a@b.c"))).attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(error: MongoError.BulkWriteFailed) =>
          error.duplicateKeys.map(_.index) shouldBe List(1)
        case other                                   => fail(s"expected a BulkWriteFailed, got $other")
      }
    }

    "keep the driver's own exception as the cause" in {
      val program =
        for
          collection <- unique("cause")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.insertOne(person("a@b.c")).attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(error: MongoError) => error.getCause shouldBe a[com.mongodb.MongoWriteException]
        case other                   => fail(s"expected a MongoError, got $other")
      }
    }
  }

  "a write concern the server cannot satisfy" should {

    "arrive as a BulkWriteFailed from insertMany with no failed document and the write-concern failure in it" in {
      val program =
        for
          collection <- unique("unsatisfiable_concern")
          failure    <- collection.withWriteConcern(WriteConcern(2).withWTimeout(1, TimeUnit.SECONDS)).insertMany(List(person("a@b.c"))).attempt
          stored     <- collection.count()
        yield (failure, stored)

      program.timeout(30.seconds).asserting {
        case (Left(error: MongoError.BulkWriteFailed), stored) =>
          error.failures shouldBe Nil
          error.writeConcernErrors.map(_.code) should not be empty
          stored shouldBe 1L
        case other                                             => fail(s"expected a BulkWriteFailed, got $other")
      }
    }
  }

  "an operation that runs past the client's timeoutMS" should {

    "arrive as MongoError.ExecutionTimeout rather than as an unreachable server" in {
      val slow = Filter.Raw[BsonDocument](BsonDocument("$where", BsonString("sleep(2000) || true")))

      val program =
        for
          client     <- MongoClient.fromSettings[IO, S](
                          MongoClientSettings.builder().applyConnectionString(ConnectionString(container.getConnectionString)).timeout(500, TimeUnit.MILLISECONDS).build()
                        )
          database   <- client.getDatabase("mongo_error_it")
          collection <- database.getCollection[BsonDocument]("csot")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.find(slow).all.attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(error: MongoError.ExecutionTimeout) => error.cause shouldBe a[com.mongodb.MongoOperationTimeoutException]
        case other                                    => fail(s"expected an ExecutionTimeout, got $other")
      }
    }

    "arrive the same way from a timeout set on the database or the collection alone" in {
      val slow = Filter.Raw[BsonDocument](BsonDocument("$where", BsonString("sleep(2000) || true")))

      val program =
        for
          client        <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database      <- client.getDatabase("mongo_error_it")
          collection    <- database.getCollection[BsonDocument]("csot_scoped")
          _             <- collection.insertOne(person("a@b.c"))
          viaDatabase   <- database.withTimeout(500.millis).getCollection[BsonDocument]("csot_scoped").flatMap(_.find(slow).all.attempt)
          viaCollection <- collection.withTimeout(500.millis).find(slow).all.attempt
          unbounded     <- collection.find(Filter.all).all
          _             <- client.close
        yield (viaDatabase, viaCollection, unbounded.size)

      program.timeout(30.seconds).asserting {
        case (Left(_: MongoError.ExecutionTimeout), Left(_: MongoError.ExecutionTimeout), 1) => succeed
        case other                                                                           => fail(s"expected two ExecutionTimeouts, got $other")
      }
    }
  }

  "a command the server rejects" should {

    "arrive as MongoError.Failed, keeping the server's code" in {
      val program =
        for
          collection <- unique("bad_hint")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.find().hint(BsonDocument("missing", BsonInt32(1))).all.attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(error: MongoError.Failed) => error.code should not be 0
        case other                          => fail(s"expected a Failed, got $other")
      }
    }

    "arrive typed through a stream as well" in {
      val program =
        for
          collection <- unique("bad_hint_stream")
          _          <- collection.insertOne(person("a@b.c"))
          failure    <- collection.find().hint(BsonDocument("missing", BsonInt32(1))).stream.compile.toList.attempt
        yield failure

      program.timeout(30.seconds).asserting {
        case Left(_: MongoError) => succeed
        case other               => fail(s"expected a MongoError, got $other")
      }
    }
  }
