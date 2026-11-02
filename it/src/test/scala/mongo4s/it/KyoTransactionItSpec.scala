package mongo4s.it

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import scala.util.Try

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.mongodb.MongoDBContainer

import kyo.{AllowUnsafe, Async, Duration as KyoDuration, Fiber, KyoApp, Sync as KyoSync}
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.kyo.{KIO, KStream}
import mongo4s.{Effect, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.kyo.KyoInstances.given

import TransactionItSpec.Person

object KyoTransactionItSpec:
  final case class Boom() extends RuntimeException("boom")

final class KyoTransactionItSpec extends AnyWordSpec, Matchers, BeforeAndAfterAll:
  import KyoTransactionItSpec.Boom

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  private val F = summon[Effect[KIO]]

  private def run[A](fa: KIO[A]): A =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  private def connect: KIO[MongoClient[KIO, KStream]] = MongoClient.fromConnectionString[KIO, KStream](container.getConnectionString)

  private def people(client: MongoClient[KIO, KStream], name: String): KIO[MongoCollection[KIO, KStream, Person]] =
    F.flatMap(client.getDatabase("kyo_tx_it"))(_.getCollection[Person](name))

  private def closed(session: ClientSession): Boolean = Try(session.getServerSession).isFailure

  "MongoClient.withTransaction on kyo" should {

    "commit a successful body" in {
      val committed = run(
        for
          client     <- connect
          collection <- people(client, "commit")
          _          <- client.withTransaction(collection.insertOne(Person("carol", 40)))
          count      <- collection.count()
          _          <- client.close
        yield count
      )

      committed shouldBe 1L
    }

    "abort and re-raise when the body fails, leaving nothing visible" in {
      val (outcome, count) = run(
        for
          client     <- connect
          collection <- people(client, "abort")
          outcome    <- F.attempt(client.withTransaction(F.flatMap(collection.insertOne(Person("dave", 50)))(_ => F.raiseError[Unit](Boom()))))
          count      <- collection.count()
          _          <- client.close
        yield (outcome, count)
      )

      outcome.left.map(_.getMessage) shouldBe Left("boom")
      count shouldBe 0L
    }
  }

  "MongoClient.withSession on kyo" should {

    "close its session after a body that succeeds and after one that fails" in {
      val seen = AtomicReference[List[ClientSession]](Nil)

      def remember(using session: Option[ClientSession]): KIO[Unit] = F.delay(seen.updateAndGet(session.toList ++ _): Unit)

      val outcome = run(
        for
          client <- connect
          _      <- client.withSession(remember)
          failed <- F.attempt(client.withSession(F.flatMap(remember)(_ => F.raiseError[Unit](Boom()))))
          _      <- client.close
        yield failed
      )

      outcome.left.map(_.getMessage) shouldBe Left("boom")
      seen.get.map(closed) shouldBe List(true, true)
    }

    "close the session of a body that is interrupted" in {
      val seen    = AtomicReference[Option[ClientSession]](None)
      val started = CountDownLatch(1)

      val client = run(connect)

      try
        run(
          for
            fiber <- Fiber.initUnscoped(
                       client.withSession(F.flatMap(F.delay { seen.set(summon[Option[ClientSession]]); started.countDown() })(_ => Async.never[Unit]))
                     )
            _     <- KyoSync.defer(started.await())
            _     <- fiber.interrupt
          yield ()
        )

        val deadline = System.nanoTime() + 5.seconds.toNanos
        while !seen.get.exists(closed) && System.nanoTime() < deadline do Thread.sleep(10)

        seen.get.map(closed) shouldBe Some(true)
      finally run(client.close)
    }
  }
