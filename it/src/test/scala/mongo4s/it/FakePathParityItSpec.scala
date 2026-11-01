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
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object FakePathParityItSpec:

  def array(values: BsonValue*): BsonArray = BsonArray(values.toList.asJava)

  def named(name: String): BsonDocument = BsonDocument("name", BsonString(name))

final class FakePathParityItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import FakePathParityItSpec.*

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]
  type Coll = MongoCollection[IO, S, BsonDocument]

  private def real(name: String, seed: List[BsonDocument]): IO[Coll] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("fake_path_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertMany(seed.map(_.clone()))
    yield collection

  private def fake(seed: List[BsonDocument]): IO[Coll] =
    val collection = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])
    collection.insertMany(seed.map(_.clone())).as(collection)

  private def names(name: String, seed: List[BsonDocument])(filter: Filter[BsonDocument]): IO[(List[String], List[String])] =
    def read(collection: Coll) = collection.find(filter).all.map(_.map(_.getString("name").getValue).sorted)

    for
      server <- real(name, seed).flatMap(read)
      double <- fake(seed).flatMap(read)
    yield (server, double)

  private def matchesOn(name: String, seed: List[BsonDocument], filter: Filter[BsonDocument])(expected: String*) =
    names(name, seed)(filter).timeout(30.seconds).asserting((server, double) => (server, double) shouldBe (expected.toList, expected.toList))

  private def int(path: String)  = Field.stored[BsonDocument, Int](path)
  private def text(path: String) = Field.stored[BsonDocument, String](path)

  "a path segment that is a number" should {
    val seed = List(
      named("first").append("items", array(named("pen"), named("cup"))),
      named("second").append("items", array(named("cup"), named("pen"))),
      named("keyed").append("items", array(BsonDocument("0", named("pen")))),
      named("scalars").append("scores", array(BsonInt32(1), BsonInt32(9))),
      named("reversed").append("scores", array(BsonInt32(9), BsonInt32(1))),
    )

    "pick the element at that position, and a field of that name in an element" in {
      matchesOn("index_doc", seed, text("items.0.name").equalTo("pen"))("first", "keyed")
    }

    "pick a scalar element by position" in {
      matchesOn("index_scalar", seed, int("scores.1").gt(5))("scalars")
    }
  }

  "an operator on a path through an array of documents" should {
    val seed = List(
      named("pair").append("items", array(BsonDocument("tags", array(BsonString("x"), BsonString("y"))).append("qty", BsonInt32(4)))),
      named("single").append("items", array(BsonDocument("tags", array(BsonString("x"))).append("qty", BsonInt32(3)))),
    )

    "apply $size to each element's array" in {
      matchesOn("size_through", seed, Field.stored[BsonDocument, List[String]]("items.tags").hasSize(2))("pair")
    }

    "apply $mod to each element's value" in {
      matchesOn("mod_through", seed, int("items.qty").mod(2, 0))("pair")
    }

    "apply $elemMatch to each element's array" in {
      matchesOn("elem_match_through", seed, Field.stored[BsonDocument, List[String]]("items.tags").elemMatch(Field.self[String].equalTo("y")))("pair")
    }

    "apply $type to each element's value" in {
      matchesOn("type_through", seed, Field.stored[BsonDocument, Any]("items.qty").hasType(mongo4s.bson.BsonTypeName.Int))("pair", "single")
    }
  }

  "an operator on an array of scalars" should {
    val seed = List(
      named("mixed").append("v", array(BsonInt32(1), BsonString("s"), BsonInt32(6))),
      named("numbers").append("v", array(BsonInt32(1), BsonInt32(3))),
      named("scalar").append("v", BsonString("s")),
    )
    val v    = Field.stored[BsonDocument, Any]("v")

    "match $type on any element, and array on the array itself" in {
      matchesOn("type_elements", seed, v.hasType(mongo4s.bson.BsonTypeName.String))("mixed", "scalar")
    }

    "match $type array on the array" in {
      matchesOn("type_array", seed, v.hasType(mongo4s.bson.BsonTypeName.Array))("mixed", "numbers")
    }

    "match $mod on any element" in {
      matchesOn("mod_elements", seed, int("v").mod(2, 0))("mixed")
    }

    "match $bitsAllSet on any element" in {
      matchesOn("bits_elements", seed, int("v").bitsAllSet(6))("mixed")
    }
  }

  "$all" should {
    val seed = List(
      named("scalar").append("tag", BsonString("a")),
      named("array").append("tag", array(BsonString("a"), BsonString("b"))),
      named("other").append("tag", BsonString("b")),
    )

    "match a scalar field equal to its only value" in {
      matchesOn("all_scalar", seed, Field.stored[BsonDocument, List[String]]("tag").containsAll(List("a")))("array", "scalar")
    }
  }

  "$in holding a regular expression" should {
    val seed = List(named("apple"), named("apricot"), named("banana"), named("cherry"))

    "match the strings the expression matches, beside the plain values" in {
      matchesOn("in_regex", seed, Filter.In(text("name").path, List(BsonRegularExpression("^ap"), BsonString("cherry"))))(
        "apple",
        "apricot",
        "cherry",
      )
    }

    "leave them out under $nin" in {
      matchesOn("nin_regex", seed, Filter.Nin(text("name").path, List(BsonRegularExpression("^ap"), BsonString("cherry"))))("banana")
    }
  }

  "strings" should {
    val seed = List(named("\uFF5E"), named("\uD83D\uDE00"), named("z"))

    "compare by their UTF-8 bytes, as the server does, in a sort and in a range" in {
      def read(collection: Coll) =
        for
          sorted <- collection.find().sort(Sort.asc(text("name"))).all.map(_.map(_.getString("name").getValue))
          above  <- collection.find(text("name").gt("\uFF5E")).all.map(_.map(_.getString("name").getValue))
        yield (sorted, above)

      val program =
        for
          server <- real("utf8_order", seed).flatMap(read)
          double <- fake(seed).flatMap(read)
        yield (server, double)

      program
        .timeout(30.seconds)
        .asserting((server, double) => (server, double) shouldBe ((List("z", "\uFF5E", "\uD83D\uDE00"), List("\uD83D\uDE00")), server))
    }
  }

  "a path through an array nested in an array" should {
    val seed = List(
      named("nested").append("a", array(array(BsonDocument("b", BsonInt32(1))))),
      named("flat").append("a", array(BsonDocument("b", BsonInt32(1)))),
    )

    "not descend into the inner array" in {
      matchesOn("nested_arrays", seed, int("a.b").equalTo(1))("flat")
    }
  }
