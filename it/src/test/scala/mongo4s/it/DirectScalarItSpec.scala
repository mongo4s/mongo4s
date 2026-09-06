package mongo4s.it

import java.util.UUID
import java.time.Instant

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonDateTime, BsonDocument, BsonInt32, BsonObjectId, BsonString, BsonType}
import org.bson.types.ObjectId

import mongo4s.bson.direct.WireCodec
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object DirectScalarItSpec:

  final case class Audit(_id: ObjectId, session: UUID, at: Instant, amount: BigDecimal) derives WireCodec

final class DirectScalarItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import DirectScalarItSpec.*

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val session = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
  private val epoch   = Instant.ofEpochMilli(1_733_000_000_000L)

  private def audit(offsetMillis: Long, amount: BigDecimal): Audit =
    Audit(ObjectId.get(), session, epoch.plusMillis(offsetMillis), amount)

  private val early = audit(0L, BigDecimal("10.50"))
  private val late  = audit(60_000L, BigDecimal("1234.56"))

  private def seeded(name: String): IO[(MongoCollection[IO, S, Audit], MongoCollection[IO, S, BsonDocument])] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("direct_scalar_it")
      collection <- database.getDirectCollection[Audit](name)
      documents  <- database.getCollection[BsonDocument](name)
      _          <- collection.insertMany(List(early, late))
    yield (collection, documents)

  "a direct collection carrying ObjectId, UUID, Instant and BigDecimal" should {

    "round-trip every field through the wire codec" in {
      val program =
        for
          (collection, _) <- seeded("round_trip")
          found           <- collection.find(Field.of[Audit, ObjectId](_._id).equalTo(late._id)).all
        yield found

      program.timeout(30.seconds).asserting(_ shouldBe List(late))
    }

    "store the BSON types the server indexes on" in {
      val program =
        for
          (_, documents) <- seeded("stored_types")
          stored         <- documents.find(Field.stored[BsonDocument, ObjectId]("_id").equalTo(early._id)).first
        yield stored.map { document =>
          List("_id", "session", "at", "amount").map(field => field -> document.get(field).getBsonType)
        }

      program
        .timeout(30.seconds)
        .asserting(
          _ shouldBe Some(
            List(
              "_id"     -> BsonType.OBJECT_ID,
              "session" -> BsonType.STRING,
              "at"      -> BsonType.DATE_TIME,
              "amount"  -> BsonType.DECIMAL128,
            )
          )
        )
    }

    "read a number the server stored in a narrower width than the model declares" in {
      val program =
        for
          (collection, documents) <- seeded("narrow_width")
          _                       <- documents.insertOne(
                                       BsonDocument("_id", BsonObjectId(ObjectId.get()))
                                         .append("session", BsonString(session.toString))
                                         .append("at", BsonDateTime(epoch.toEpochMilli))
                                         .append("amount", BsonInt32(42))
                                     )
          found                   <- collection.find(Field.of[Audit, BigDecimal](_.amount).equalTo(BigDecimal(42))).all
        yield found.map(_.amount)

      program.timeout(30.seconds).asserting(_ shouldBe List(BigDecimal(42)))
    }

    "compare a date server-side rather than lexicographically" in {
      val program =
        for
          (collection, _) <- seeded("date_range")
          found           <- collection.find(Field.of[Audit, Instant](_.at).gt(epoch.plusMillis(1L))).all
        yield found.map(_.at)

      program.timeout(30.seconds).asserting(_ shouldBe List(late.at))
    }

    "compare a decimal server-side rather than lexicographically" in {
      val program =
        for
          (collection, _) <- seeded("decimal_range")
          found           <- collection.find(Field.of[Audit, BigDecimal](_.amount).gt(BigDecimal("100"))).all
        yield found.map(_.amount)

      program.timeout(30.seconds).asserting(_ shouldBe List(late.amount))
    }
  }
