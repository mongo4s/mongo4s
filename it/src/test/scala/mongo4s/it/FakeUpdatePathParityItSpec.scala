package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.*

import scala.jdk.CollectionConverters.given

import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.{Field, MongoClient, MongoCollection, MongoError}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object FakeUpdatePathParityItSpec:

  def array(values: BsonValue*): BsonArray = BsonArray(values.toList.asJava)

  def item(name: String, qty: Int): BsonDocument = BsonDocument("name", BsonString(name)).append("qty", BsonInt32(qty))

final class FakeUpdatePathParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import FakeUpdatePathParityItSpec.*

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private def real(name: String, seed: BsonDocument): IO[Coll] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_update_path_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertOne(seed.clone())
    yield collection

  private def fake(seed: BsonDocument): IO[Coll] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])
    collection.insertOne(seed.clone()).as(collection)

  private def withoutId(document: BsonDocument): BsonDocument =
    val copy = BsonDocument()
    document.entrySet.forEach(entry => if entry.getKey != "_id" then copy.append(entry.getKey, entry.getValue): Unit)
    copy

  private def outcome(collection: Coll, update: Update[BsonDocument]): IO[Either[Option[Int], BsonDocument]] =
    collection.updateOne(Filter.all, update).attempt.flatMap {
      case Left(failed: MongoError) => IO.pure(Left(Some(failed.code)))
      case Left(_)                  => IO.pure(Left(None))
      case Right(_)                 => collection.find().first.map(found => Right(withoutId(found.get)))
    }

  private def agree(name: String, seed: BsonDocument, update: Update[BsonDocument])(expected: Either[Option[Int], BsonDocument]) =
    val program =
      for
        server <- real(name, seed).flatMap(outcome(_, update))
        double <- fake(seed).flatMap(outcome(_, update))
      yield (server, double)

    program.timeout(30.seconds).asserting((server, double) => (server, double) shouldBe (expected, expected))

  private def int(path: String) = Field.stored[BsonDocument, Int](path)

  private val basket = BsonDocument("name", BsonString("basket")).append("items", array(item("pen", 1), item("cup", 2)))

  "an update through an array" should {

    "set a field of the element at a position" in {
      agree("set_position", basket, Update.set(int("items.0.qty"), 9))(
        Right(BsonDocument("name", BsonString("basket")).append("items", array(item("pen", 9), item("cup", 2))))
      )
    }

    "pad the array with nulls to set a position past its end" in {
      agree("set_past_end", BsonDocument("tags", array(BsonString("a"))), Update.set(Field.stored[BsonDocument, String]("tags.2"), "c"))(
        Right(BsonDocument("tags", array(BsonString("a"), BsonNull.VALUE, BsonString("c"))))
      )
    }

    "increment a field of the element at a position" in {
      agree("inc_position", basket, Update.inc(int("items.1.qty"), 5))(
        Right(BsonDocument("name", BsonString("basket")).append("items", array(item("pen", 1), item("cup", 7))))
      )
    }

    "unset a field of the element at a position" in {
      agree("unset_position_field", basket, Update.unset(int("items.0.qty")))(
        Right(BsonDocument("name", BsonString("basket")).append("items", array(BsonDocument("name", BsonString("pen")), item("cup", 2))))
      )
    }

    "leave a null where an unset element was" in {
      agree("unset_position", basket, Update.unset(Field.stored[BsonDocument, Any]("items.0")))(
        Right(BsonDocument("name", BsonString("basket")).append("items", array(BsonNull.VALUE, item("cup", 2))))
      )
    }
  }

  "an update that cannot reach its path" should {

    "be refused with the server's code when a field sits in the way" in {
      agree("set_over_scalar", basket, Update.set(Field.stored[BsonDocument, String]("name.first"), "x"))(Left(Some(28)))
    }

    "be refused with the server's code when null sits in the way" in {
      agree("set_over_null", BsonDocument("name", BsonNull.VALUE), Update.set(Field.stored[BsonDocument, String]("name.first"), "x"))(Left(Some(28)))
    }

    "be refused with the server's code when an array would need a named field" in {
      agree("set_name_in_array", basket, Update.set(int("items.qty"), 3))(Left(Some(28)))
    }
  }

  "arithmetic on a field that is not a number" should {

    "be refused with the server's code for $inc" in {
      agree("inc_string", basket, Update.inc(int("name"), 1))(Left(Some(14)))
    }

    "be refused with the server's code for $mul" in {
      agree("mul_string", basket, Update.mul(int("name"), 2))(Left(Some(14)))
    }
  }

  "$push with a negative position" should {

    "count the position from the end" in {
      val tags = Field.stored[BsonDocument, List[String]]("tags")

      agree(
        "push_negative",
        BsonDocument("tags", array(BsonString("b"), BsonString("a"), BsonString("c"))),
        Update.pushAll(tags, List("x"), PushOptions.default[String].withPosition(-1)),
      )(Right(BsonDocument("tags", array(BsonString("b"), BsonString("a"), BsonString("x"), BsonString("c")))))
    }

    "insert first when it counts past the start" in {
      val tags = Field.stored[BsonDocument, List[String]]("tags")

      agree("push_negative_past", BsonDocument("tags", array(BsonString("b"))), Update.pushAll(tags, List("x"), PushOptions.default[String].withPosition(-5)))(
        Right(BsonDocument("tags", array(BsonString("x"), BsonString("b"))))
      )
    }
  }

  "$pull by a document" should {

    "remove each element the document matches as a query, extra fields and all" in {
      agree("pull_document", basket, Update.Pull(Field.stored[BsonDocument, Any]("items").path, BsonDocument("name", BsonString("pen"))))(
        Right(BsonDocument("name", BsonString("basket")).append("items", array(item("cup", 2))))
      )
    }
  }

  "the array operators" should {
    val widths = BsonDocument("v", array(BsonInt64(1L), BsonDouble(1.0), BsonInt32(2)))
    val v      = Field.stored[BsonDocument, List[Int]]("v")

    "pull every width of an equal number" in {
      agree("pull_widths", widths, Update.pull(v, 1))(Right(BsonDocument("v", array(BsonInt32(2)))))
    }

    "pullAll every width of an equal number" in {
      agree("pull_all_widths", widths, Update.pullAll(v, List(1)))(Right(BsonDocument("v", array(BsonInt32(2)))))
    }

    "not add a number another width of it is already there" in {
      agree("add_to_set_widths", widths, Update.addToSet(v, 1))(Right(widths))
    }
  }

  "an insert" should {
    "store the _id first, as the server does, whether it was given or generated" in {
      def keys(collection: Coll) =
        collection.insertOne(BsonDocument("name", BsonString("late")).append("_id", BsonString("given"))) *>
          collection.find().all.map(_.map(_.keySet.asScala.toList.head))

      val program =
        for
          server <- real("id_first", basket).flatMap(keys)
          double <- fake(basket).flatMap(keys)
        yield (server, double)

      program.timeout(30.seconds).asserting(_ shouldBe (List("_id", "_id"), List("_id", "_id")))
    }
  }

  "replaceOne" should {
    def modified(collection: Coll, replacement: BsonArray): IO[Long] =
      collection.find().first.flatMap { stored =>
        val id = stored.get.get("_id")
        collection.replaceOne(Filter.all, BsonDocument("_id", id).append("name", BsonString("basket")).append("items", replacement)).map(_.modifiedCount)
      }

    "count an identical replacement as matched, not modified" in {
      val program =
        for
          server <- real("replace_same", basket).flatMap(modified(_, basket.getArray("items")))
          double <- fake(basket).flatMap(modified(_, basket.getArray("items")))
        yield (server, double)

      program.timeout(30.seconds).asserting(_ shouldBe (0L, 0L))
    }

    "count a changed one as modified" in {
      val program =
        for
          server <- real("replace_changed", basket).flatMap(modified(_, array(item("pen", 5))))
          double <- fake(basket).flatMap(modified(_, array(item("pen", 5))))
        yield (server, double)

      program.timeout(30.seconds).asserting(_ shouldBe (1L, 1L))
    }
  }
