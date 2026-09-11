package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.*

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class FakeUpdateParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private val nameF  = Field.stored[BsonDocument, String]("name")
  private val ageF   = Field.stored[BsonDocument, Int]("age")
  private val priceF = Field.stored[BsonDocument, Double]("price")
  private val tagsF  = Field.stored[BsonDocument, List[String]]("tags")
  private val altF   = Field.stored[BsonDocument, String]("alt")
  private val seenF  = Field.stored[BsonDocument, java.time.Instant]("seen")

  private def strings(values: String*): BsonArray =
    BsonArray(values.toList.map(v => BsonString(v): BsonValue).asJava)

  private val seed =
    BsonDocument("name", BsonString("a"))
      .append("age", BsonInt32(10))
      .append("price", BsonDouble(2.0))
      .append("tags", strings("b", "a", "c"))

  private def real(name: String): IO[Coll] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_update_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertOne(seed.clone())
    yield collection

  private def fake: IO[Coll] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]], _ => fs2.Stream.empty)
    collection.insertOne(seed.clone()).as(collection)

  private def without(document: BsonDocument, keys: String*): BsonDocument =
    val copy = BsonDocument()
    document.entrySet.forEach(entry => if !keys.contains(entry.getKey) then copy.append(entry.getKey, entry.getValue): Unit)
    copy

  private def agree(name: String, update: Update[BsonDocument], volatileKeys: String*) =
    val applied = (collection: Coll) => collection.updateOne(nameF.equalTo("a"), update) *> collection.find().first.map(_.get)

    (for
      server <- real(name).flatMap(applied)
      double <- fake.flatMap(applied)
    yield (without(server, ("_id" +: volatileKeys)*), without(double, ("_id" +: volatileKeys)*)))
      .timeout(30.seconds)
      .asserting((server, double) => double shouldBe server)

  "the fake and the server" should {

    "agree on $mul" in { agree("mul", Update.mul(ageF, 3)) }

    "agree on $mul over a Double" in { agree("mul_double", Update.mul(priceF, 2.5)) }

    "agree on $min when the value is lower" in { agree("min_lower", Update.min(ageF, 5)) }

    "agree on $min when the value is higher" in { agree("min_higher", Update.min(ageF, 50)) }

    "agree on $max when the value is higher" in { agree("max_higher", Update.max(ageF, 50)) }

    "agree on $max against a missing field" in { agree("max_missing", Update.max(seenF, java.time.Instant.ofEpochMilli(1000))) }

    "agree on $rename" in { agree("rename", Update.rename(nameF, altF)) }

    "agree on $rename of a field that is not there" in { agree("rename_missing", Update.rename(altF, nameF)) }

    "agree on $push" in { agree("push", Update.push(tagsF, "d")) }

    "agree on $push into a field that is not there" in {
      agree("push_missing", Update.push(Field.stored[BsonDocument, List[String]]("extra"), "d"))
    }

    "agree on $pull" in { agree("pull", Update.pull(tagsF, "a")) }

    "agree on $pullAll" in { agree("pull_all", Update.pullAll(tagsF, List("a", "c"))) }

    "agree on $pop from the front" in { agree("pop_first", Update.popFirst(tagsF)) }

    "agree on $pop from the back" in { agree("pop_last", Update.popLast(tagsF)) }

    "agree on $addToSet when the value is new" in { agree("add_new", Update.addToSet(tagsF, "d")) }

    "agree on $addToSet when the value is already there" in { agree("add_old", Update.addToSet(tagsF, "a")) }

    "agree on $addToSet with $each" in { agree("add_each", Update.addAllToSet(tagsF, List("a", "d"))) }

    "agree on $push with $each" in { agree("push_each", Update.pushAll(tagsF, List("d", "e"))) }

    "agree on $push with $each and $position" in {
      agree("push_position", Update.pushAll(tagsF, List("d"), PushOptions.default[String].withPosition(1)))
    }

    "agree on $push with $each and $slice" in {
      agree("push_slice", Update.pushAll(tagsF, List("d", "e"), PushOptions.default[String].withSlice(2)))
    }

    "agree on $push with $each sorted ascending" in {
      agree("push_sorted", Update.pushAll(tagsF, List("d"), PushOptions.default[String].sortedAscending))
    }

    "agree on $push with a negative $slice, which keeps the tail" in {
      agree("push_tail", Update.pushAll(tagsF, List("d"), PushOptions.default[String].withSlice(-2)))
    }

    "agree that $setOnInsert does nothing on a plain update" in {
      agree("set_on_insert", Update.setOnInsert(nameF, "ignored"))
    }

    "agree on $currentDate, up to the clock" in { agree("current_date", Update.currentDate(seenF), "seen") }

    "agree on several operators combined" in {
      agree("combined", Update.inc(ageF, 1).and(Update.push(tagsF, "d")).and(Update.rename(nameF, altF)))
    }
  }
