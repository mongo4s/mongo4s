package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.syntax.all.*
import cats.effect.IO
import cats.effect.unsafe.implicits.given
import org.bson.{BsonBoolean, BsonDocument, BsonInt32, BsonString}

import mongo4s.cats.CatsStream
import mongo4s.testkit.FakeMongoCollection
import mongo4s.operations.{Filter, Index, Update}
import mongo4s.bson.BsonDocumentCodec
import mongo4s.{Field, MongoError}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

final class FakeConcurrencySpec extends AnyWordSpec, Matchers:

  type S[A] = CatsStream[IO][A]

  private val rounds = 10

  private val idF      = Field.stored[BsonDocument, Int]("_id")
  private val nF       = Field.stored[BsonDocument, Int]("n")
  private val emailF   = Field.stored[BsonDocument, String]("email")
  private val claimedF = Field.stored[BsonDocument, Boolean]("claimed")

  private def fresh: FakeMongoCollection[IO, S, BsonDocument] =
    FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])

  private def concurrently[A](n: Int)(op: Int => IO[A]): List[Either[Throwable, A]] =
    (1 to n).toList.parTraverse(i => op(i).attempt).unsafeRunSync()

  private def keyed(id: Int): BsonDocument = BsonDocument("_id", BsonInt32(id))

  "a fake hit from many fibers at once" should {
    "keep every one of many concurrent inserts" in {
      (1 to rounds).foreach { _ =>
        val c       = fresh
        val results = concurrently(2000)(i => c.insertOne(keyed(i)))

        results.count(_.isLeft) shouldBe 0
        c.estimatedCount.unsafeRunSync() shouldBe 2000L
      }
    }

    "let exactly one of many concurrent inserts with the same _id through" in {
      (1 to rounds).foreach { _ =>
        val c       = fresh
        val results = concurrently(200)(_ => c.insertOne(keyed(1)))

        results.count(_.isRight) shouldBe 1
        all(results.collect { case Left(error) => error }) shouldBe a[MongoError.DuplicateKey]
        c.estimatedCount.unsafeRunSync() shouldBe 1L
      }
    }

    "hold a unique index against concurrent inserts of the same key" in {
      (1 to rounds).foreach { _ =>
        val c = fresh
        c.createIndex(Index.unique(emailF)).unsafeRunSync()

        val results = concurrently(200)(i => c.insertOne(keyed(i).append("email", BsonString("a@b.c"))))

        results.count(_.isRight) shouldBe 1
        c.estimatedCount.unsafeRunSync() shouldBe 1L
      }
    }

    "lose no increment when many updates hit one document together" in {
      (1 to rounds).foreach { _ =>
        val c = fresh
        c.insertOne(keyed(1).append("n", BsonInt32(0))).unsafeRunSync()

        val results = concurrently(1000)(_ => c.updateOne(idF.equalTo(1), Update.inc(nF, 1)))

        results.count(_.isLeft) shouldBe 0
        c.find(idF.equalTo(1)).first.unsafeRunSync().map(_.getInt32("n").getValue) shouldBe Some(1000)
      }
    }

    "hand each document to one findOneAndUpdate only, the way a work queue claims jobs" in {
      (1 to rounds).foreach { _ =>
        val c = fresh
        c.insertMany((1 to 100).map(i => keyed(i).append("claimed", BsonBoolean.FALSE))).unsafeRunSync()

        val claims = concurrently(300)(_ => c.findOneAndUpdate(claimedF.equalTo(false), Update.set(claimedF, true)))
        val ids    = claims.collect { case Right(Some(document)) => document.getInt32("_id").getValue }

        claims.count(_.isLeft) shouldBe 0
        ids should have size 100
        ids.distinct should have size 100
      }
    }

    "read whole snapshots while writes are in flight" in {
      (1 to rounds).foreach { _ =>
        val c       = fresh
        val results = concurrently(1000) { i =>
          if i % 2 == 0
          then c.insertOne(keyed(i)).void
          else (c.find(Filter.all).all, c.count()).tupled.void
        }

        results.count(_.isLeft) shouldBe 0
        c.find(Filter.all).all.unsafeRunSync().map(_.getInt32("_id").getValue) shouldBe c.snapshot.map(_.getInt32("_id").getValue)
        c.estimatedCount.unsafeRunSync() shouldBe 500L
      }
    }
  }
