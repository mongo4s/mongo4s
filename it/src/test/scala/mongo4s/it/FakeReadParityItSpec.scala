package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.*

import scala.jdk.CollectionConverters.given

import mongo4s.bson.{BsonDocumentCodec, BsonError}
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection, MongoError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object FakeReadParityItSpec:

  def array(values: BsonValue*): BsonArray = BsonArray(values.toList.asJava)

  def named(name: String): BsonDocument = BsonDocument("name", BsonString(name))

  final case class Person(name: String, age: Int)

  val people: BsonDocumentCodec[Person] = BsonDocumentCodec.make(
    person => BsonDocument("name", BsonString(person.name)).append("age", BsonInt32(person.age)),
    document =>
      if document.containsKey("age")
      then Right(Person(document.getString("name").getValue, document.getInt32("age").getValue))
      else Left(BsonError.MissingField("age")),
  )

final class FakeReadParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import FakeReadParityItSpec.*

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private def real[A](name: String, seed: List[BsonDocument], codec: BsonDocumentCodec[A]): IO[MongoCollection[IO, S, A]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_read_it")
      raw        <- database.getCollection[BsonDocument](name)
      _          <- raw.insertMany(seed.map(_.clone()))
      collection <- database.getCollection[A](name)(using codec)
    yield collection

  private def fake[A](seed: List[BsonDocument], codec: BsonDocumentCodec[A]): IO[MongoCollection[IO, S, A]] =
    val collection = FakeMongoCollection[IO, S, A](codec)
    IO(seed.foreach(document => collection.insertRaw(document.clone()))).as(collection)

  private val documents = summon[BsonDocumentCodec[BsonDocument]]

  private def agree[A](name: String, seed: List[BsonDocument])(read: Coll => IO[A]) =
    val program =
      for
        server <- real(name, seed, documents).flatMap(read)
        double <- fake(seed, documents).flatMap(read)
      yield (server, double)

    program.timeout(30.seconds).asserting((server, double) => double shouldBe server)

  private def withoutId(document: BsonDocument): BsonDocument =
    val copy = BsonDocument()
    document.entrySet.forEach(entry => if entry.getKey != "_id" then copy.append(entry.getKey, entry.getValue): Unit)
    copy

  private val basket =
    named("basket").append(
      "items",
      array(
        BsonDocument("name", BsonString("pen")).append("qty", BsonInt32(1)),
        BsonDocument("qty", BsonInt32(2)),
        BsonInt32(5),
        array(BsonDocument("name", BsonString("deep")).append("qty", BsonInt32(3))),
      ),
    )

  "a projection through an array" should {

    "include the field from each element, as the server shapes it" in {
      agree("include_through", List(basket)) {
        _.find().projection(Projection.empty[BsonDocument].include(Field.stored[BsonDocument, String]("items.name")).withoutId).all
      }
    }

    "exclude the field from each element, as the server shapes it" in {
      agree("exclude_through", List(basket)) {
        _.find().projection(Projection.empty[BsonDocument].exclude(Field.stored[BsonDocument, Int]("items.qty"))).all.map(_.map(withoutId))
      }
    }
  }

  "distinct" should {

    "read a dotted path through an array of documents" in {
      agree("distinct_through", List(basket, named("other").append("items", array(named("cup"), named("pen"))))) {
        _.distinct(Field.stored[BsonDocument, String]("items.name")).all.map(_.sorted)
      }
    }

    "count numbers of different widths as one value" in {
      val seed = List(BsonDocument("v", BsonInt32(1)), BsonDocument("v", BsonInt64(1L)), BsonDocument("v", BsonDouble(1.0)), BsonDocument("v", BsonInt32(2)))

      agree("distinct_widths", seed) { collection =>
        collection.distinct(Field.stored[BsonDocument, Double]("v")).all.map(_.sorted)
      }
    }
  }

  "a $group" should {

    "put numbers of different widths in one bucket" in {
      val seed = List(BsonDocument("v", BsonInt32(1)), BsonDocument("v", BsonInt64(1L)), BsonDocument("v", BsonDouble(1.0)), BsonDocument("v", BsonInt32(2)))

      agree("group_widths", seed) {
        _.aggregate[BsonDocument](
          Seq(
            Stage.groupBy(Field.stored[BsonDocument, Double]("v"))("n" -> Accumulator.count),
            Stage.sortBy(Sort.asc(Field.stored[BsonDocument, Double]("_id"))),
          )
        ).all.map(_.map(group => (group.get("_id").asNumber.doubleValue, group.getInt32("n").getValue)))
      }
    }
  }

  "first" should {

    "read the first match without decoding the ones after it" in {
      val seed    = List(named("ok").append("age", BsonInt32(1)), named("broken"))
      val program =
        for
          server <- real("first_decodes_one", seed, people).flatMap(_.find().first)
          double <- fake(seed, people).flatMap(_.find().first)
        yield (server, double)

      program.timeout(30.seconds).asserting(_ shouldBe (Some(Person("ok", 1)), Some(Person("ok", 1))))
    }
  }

  "a bulk write that fails on something other than a duplicate" should {
    val seed = List(BsonDocument("_id", BsonString("a")).append("name", BsonString("a")))
    val name = Field.stored[BsonDocument, String]("name")

    def outcome(ordered: Boolean)(collection: Coll) =
      val commands = List(
        WriteCommand.replaceOne(name.equalTo("a"), BsonDocument("_id", BsonString("moved")).append("name", BsonString("a"))),
        WriteCommand.InsertOne(BsonDocument("_id", BsonString("b")).append("name", BsonString("b"))),
      )

      for
        result <- collection.bulkWrite(commands, if ordered then BulkWriteOptions.default else BulkWriteOptions.unordered).attempt
        stored <- collection.find().all.map(_.map(_.getString("_id").getValue).sorted)
      yield result match
        case Left(failed: MongoError.BulkWriteFailed) => (failed.failures.map(failure => (failure.index, failure.code)), failed.result.insertedCount, stored)
        case other                                    => fail(s"expected a BulkWriteFailed, got $other")

    "report it as a failure of that command and stop an ordered write there" in {
      agree("bulk_other_ordered", seed)(outcome(ordered = true))
    }

    "report it and go on with an unordered write" in {
      agree("bulk_other_unordered", seed)(outcome(ordered = false))
    }
  }

  "explain and watch" should {

    "be refused inside the effect, where attempt sees them" in {
      val collection = FakeMongoCollection[IO, S, BsonDocument](documents)

      for
        explained <- IO(collection.find().explain()).flatMap(_.attempt)
        watched   <- IO(collection.watch()).flatMap(_.compile.toList.attempt)
      yield
        explained.left.map(_.getClass) shouldBe Left(classOf[UnsupportedOperationException])
        watched.left.map(_.getClass) shouldBe Left(classOf[UnsupportedOperationException])
    }
  }
