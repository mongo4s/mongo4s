package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import cats.syntax.all.*

import mongo4s.cats.CatsStream
import mongo4s.bson.direct.WireCodec
import mongo4s.results.ClientBulkWriteResult
import mongo4s.{Field, MongoClient, MongoError}
import mongo4s.operations.{BulkWriteOptions, Index, Update, UpdateOptions, WriteCommand}

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ClientBulkWriteItSpec:
  final case class User(id: String, name: String) derives WireCodec
  final case class Order(id: String, userId: String, total: Int) derives WireCodec

final class ClientBulkWriteItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ClientBulkWriteItSpec.{Order, User}

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val userId  = Field.of[User, String](_.id)
  private val orderId = Field.of[Order, String](_.id)
  private val total   = Field.of[Order, Int](_.total)

  private def setUp(database: String) =
    for
      client <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      users  <- client.getDatabase(database).flatMap(_.getDirectCollection[User]("users"))
      orders <- client.getDatabase(s"${database}_orders").flatMap(_.getDirectCollection[Order]("orders"))
      _      <- users.createIndex(Index.unique(userId))
      _      <- orders.insertOne(Order("o1", "u0", 10))
    yield (client, users, orders)

  "a client bulk write" should {
    "write to collections of different entities, in different databases, in one command" in {
      val program =
        for
          (client, users, orders) <- setUp("client_bulk")
          result                  <- client.bulkWrite(
                                       List(
                                         users.write(WriteCommand.InsertOne(User("u1", "ada"))),
                                         orders.write(WriteCommand.InsertOne(Order("o2", "u1", 5))),
                                         orders.write(WriteCommand.updateOne(orderId.equalTo("o1"), Update.inc(total, 1))),
                                         orders.write(WriteCommand.deleteOne(orderId.equalTo("missing"))),
                                       )
                                     )
          stored                  <- (users.count(), orders.find(orderId.equalTo("o1")).first).tupled
        yield (result, stored)

      program.asserting(_ shouldBe (ClientBulkWriteResult(2, 1, 1, 0, 0), (1L, Some(Order("o1", "u0", 11)))))
    }

    "stop at a duplicate when ordered, naming the write that failed and what landed before it" in {
      val program =
        for
          (client, users, orders) <- setUp("client_bulk_ordered")
          failed                  <- client
                                       .bulkWrite(
                                         List(
                                           users.write(WriteCommand.InsertOne(User("u1", "ada"))),
                                           orders.write(WriteCommand.InsertOne(Order("o2", "u1", 1))),
                                           users.write(WriteCommand.InsertOne(User("u1", "again"))),
                                           users.write(WriteCommand.InsertOne(User("u2", "bob"))),
                                         )
                                       )
                                       .attempt
          stored                  <- users.count()
        yield (failed, stored)

      program.asserting { (failed, stored) =>
        failed match
          case Left(error: MongoError.ClientBulkWriteFailed) =>
            error.failures.map(_.index) shouldBe List(2)
            error.duplicateKeys.size shouldBe 1
            error.partialResult.map(_.insertedCount) shouldBe Some(2L)
          case other                                         => fail(s"expected ClientBulkWriteFailed, got $other")
        stored shouldBe 1L
      }
    }

    "carry on past a duplicate when unordered" in {
      val program =
        for
          (client, users, _) <- setUp("client_bulk_unordered")
          failed             <- client
                                  .bulkWrite(
                                    List(
                                      users.write(WriteCommand.InsertOne(User("u1", "ada"))),
                                      users.write(WriteCommand.InsertOne(User("u1", "again"))),
                                      users.write(WriteCommand.InsertOne(User("u2", "bob"))),
                                    ),
                                    BulkWriteOptions.unordered,
                                  )
                                  .attempt
          stored             <- users.count()
        yield (failed.isLeft, stored)

      program.asserting(_ shouldBe (true, 2L))
    }

    "refuse a comment set on one command, which only the whole bulk can carry" in {
      val program =
        for
          (client, users, _) <- setUp("client_bulk_refused")
          failed             <-
            client
              .bulkWrite(
                List(users.write(WriteCommand.updateOne(userId.equalTo("u1"), Update.set(Field.of[User, String](_.name), "x"), UpdateOptions.default.withComment("no"))))
              )
              .attempt
        yield failed.left.toOption.map(_.getClass.getSimpleName)

      program.asserting(_ shouldBe Some("IllegalArgumentException"))
    }

    "take part in a transaction" in {
      val program =
        for
          (client, users, _) <- setUp("client_bulk_transaction")
          aborted            <- client
                                  .withTransaction(
                                    client.bulkWrite(List(users.write(WriteCommand.InsertOne(User("u1", "ada"))))) >> IO.raiseError(RuntimeException("rollback"))
                                  )
                                  .attempt
          stored             <- users.count()
        yield (aborted.isLeft, stored)

      program.asserting(_ shouldBe (true, 0L))
    }
  }
