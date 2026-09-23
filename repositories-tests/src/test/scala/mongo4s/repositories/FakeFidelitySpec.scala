package mongo4s.repositories

import java.time.Instant

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.given
import org.bson.*
import org.bson.types.ObjectId

import scala.jdk.CollectionConverters.given

import mongo4s.bson.{BsonDocumentCodec, BsonInstances}
import mongo4s.cats.CatsStream
import mongo4s.operations.*
import mongo4s.testkit.FakeMongoCollection
import mongo4s.Field

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object FakeFidelitySpec:
  def arr(values: String*): BsonArray = BsonArray(values.toList.map(v => BsonString(v): BsonValue).asJava)

  def docs(values: (String, Int)*): BsonArray =
    BsonArray(values.toList.map((n, q) => BsonDocument("name", BsonString(n)).append("qty", BsonInt32(q)): BsonValue).asJava)

  def keyed(id: String): BsonDocument = BsonDocument("_id", BsonString(id))

final class FakeFidelitySpec extends AnyWordSpec, Matchers:
  import FakeFidelitySpec.*

  type S[A] = CatsStream[IO][A]

  private def fake(seed: BsonDocument*): FakeMongoCollection[IO, S, BsonDocument] =
    val c = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]])
    c.insertMany(seed.toList).unsafeRunSync()
    c

  private val tagsF  = Field.stored[BsonDocument, List[String]]("tags")
  private val nameF  = Field.stored[BsonDocument, String]("name")
  private val ageF   = Field.stored[BsonDocument, Int]("age")
  private val priceF = Field.stored[BsonDocument, Double]("price")
  private val nickF  = Field.stored[BsonDocument, String]("nick")
  private val seenF  = Field.stored[BsonDocument, Instant]("seen")
  private val idF    = Field.stored[BsonDocument, ObjectId]("_id")
  private val itemF  = Field.stored[BsonDocument, List[BsonValue]]("items") / "name"

  private def found(c: FakeMongoCollection[IO, S, BsonDocument], f: Filter[BsonDocument]): Int =
    c.find(f).all.unsafeRunSync().size

  "a filter over an array field" should {
    val doc = BsonDocument("name", BsonString("foobar")).append("tags", arr("a", "b")).append("items", docs(("pen", 2)))

    "match an element, the way the server matches array membership" in {
      found(fake(doc), tagsF.contains("a")) shouldBe 1
    }

    "not match an element that is absent" in {
      found(fake(doc), tagsF.contains("z")) shouldBe 0
    }

    "treat $ne over an array as 'no element equals'" in {
      found(fake(doc), Filter.Ne(tagsF.path, BsonString("a"))) shouldBe 0
      found(fake(doc), Filter.Ne(tagsF.path, BsonString("z"))) shouldBe 1
    }

    "match $in against elements" in {
      found(fake(doc), Filter.In(tagsF.path, List(BsonString("z"), BsonString("b")))) shouldBe 1
    }

    "descend a dotted path through an array of documents" in {
      found(fake(doc), Filter.Eq(itemF.path, BsonString("pen"))) shouldBe 1
      found(fake(doc), Filter.Eq(itemF.path, BsonString("cup"))) shouldBe 0
    }
  }

  "a regex filter" should {
    val doc = BsonDocument("name", BsonString("foobar"))

    "search rather than anchor the whole string" in {
      found(fake(doc), nameF.regex("oob")) shouldBe 1
    }

    "honour the options it was given" in {
      found(fake(doc), Filter.Regex(nameF.path, "FOO", "i")) shouldBe 1
      found(fake(doc), Filter.Regex(nameF.path, "FOO", "")) shouldBe 0
    }
  }

  "$eq against null" should {
    val present = BsonDocument("name", BsonString("a")).append("nick", BsonNull.VALUE)
    val absent  = BsonDocument("name", BsonString("b"))

    "match a null and a missing field alike, as the server does" in {
      found(fake(present, absent), Filter.Eq(nickF.path, BsonNull.VALUE)) shouldBe 2
    }

    "exclude both under $ne" in {
      found(fake(present, absent), Filter.Ne(nickF.path, BsonNull.VALUE)) shouldBe 0
    }
  }

  "$inc" should {
    "keep an Int32 an Int32" in {
      val c = fake(BsonDocument("name", BsonString("a")).append("age", BsonInt32(30)))
      c.updateOne(nameF.equalTo("a"), Update.inc(ageF, 1)).unsafeRunSync()
      c.find().all.unsafeRunSync().head.get("age") shouldBe BsonInt32(31)
    }

    "find the row it just updated" in {
      val c = fake(BsonDocument("name", BsonString("a")).append("age", BsonInt32(30)))
      c.updateOne(nameF.equalTo("a"), Update.inc(ageF, 1)).unsafeRunSync()
      found(c, ageF.equalTo(31)) shouldBe 1
    }

    "add a fractional amount to a Double instead of truncating it" in {
      val c = fake(BsonDocument("name", BsonString("a")).append("price", BsonDouble(10.0)))
      c.updateOne(nameF.equalTo("a"), Update.inc(priceF, 0.5)).unsafeRunSync()
      c.find().all.unsafeRunSync().head.get("price") shouldBe BsonDouble(10.5)
    }
  }

  "$count" should {
    "report nothing when nothing reached it, as the server does" in {
      val c = fake(BsonDocument("name", BsonString("a")))
      c.aggregate[BsonDocument](Seq(Stage.matching(Filter.none[BsonDocument]), Stage.count("n"))).all.unsafeRunSync() shouldBe Nil
    }

    "still report a count when documents reached it" in {
      val c = fake(BsonDocument("name", BsonString("a")))
      c.aggregate[BsonDocument](Seq(Stage.count("n"))).all.unsafeRunSync() shouldBe
        List(BsonDocument("n", BsonInt32(1)))
    }
  }

  "findOneAndDelete" should {
    "honour the sort it was given, rather than taking insertion order" in {
      val c     = fake(
        BsonDocument("name", BsonString("a")).append("age", BsonInt32(1)),
        BsonDocument("name", BsonString("b")).append("age", BsonInt32(2)),
      )
      val taken = c.findOneAndDelete(Filter.all[BsonDocument], FindOneAndDeleteOptions.default[BsonDocument].withSort(Sort.desc(ageF))).unsafeRunSync()

      taken.map(_.getString("name").getValue) shouldBe Some("b")
    }
  }

  "ordering" should {
    "compare dates" in {
      val early = BsonDocument("name", BsonString("a")).append("seen", BsonDateTime(1000))
      val late  = BsonDocument("name", BsonString("b")).append("seen", BsonDateTime(2000))

      fake(early, late)
        .find()
        .sort(Sort.desc(seenF))
        .all
        .unsafeRunSync()
        .map(_.getString("name").getValue) shouldBe List("b", "a")

      found(fake(early, late), seenF.gt(Instant.ofEpochMilli(1500))) shouldBe 1
    }

    "compare object ids" in {
      val first  = ObjectId.get()
      Thread.sleep(5)
      val second = ObjectId.get()
      val c      = fake(BsonDocument("_id", BsonObjectId(second)), BsonDocument("_id", BsonObjectId(first)))

      c.find().sort(Sort.asc(idF)).all.unsafeRunSync().map(_.getObjectId("_id").getValue) shouldBe List(first, second)
    }

    "compare booleans" in {
      val no  = BsonDocument("name", BsonString("a")).append("ok", BsonBoolean(false))
      val yes = BsonDocument("name", BsonString("b")).append("ok", BsonBoolean(true))

      fake(no, yes)
        .find()
        .sort(Sort.desc(Field.stored[BsonDocument, Boolean]("ok")))
        .all
        .unsafeRunSync()
        .map(_.getString("name").getValue) shouldBe List("b", "a")
    }

    "order Longs past Double's precision exactly, and NaN below every number" in {
      val bigger  = BsonDocument("name", BsonString("bigger")).append("n", BsonInt64((1L << 53) + 1))
      val smaller = BsonDocument("name", BsonString("smaller")).append("n", BsonInt64(1L << 53))
      val nan     = BsonDocument("name", BsonString("nan")).append("n", BsonDouble(Double.NaN))
      val numberF = Field.stored[BsonDocument, Long]("n")

      fake(bigger, smaller, nan).find().sort(Sort.asc(numberF)).all.unsafeRunSync().map(_.getString("name").getValue) shouldBe
        List("nan", "smaller", "bigger")
    }
  }

  "equality on a number" should {
    "match the value whatever width it is stored at, as the server compares numbers" in {
      val stored = BsonDocument("name", BsonString("five")).append("n", BsonInt64(5))

      found(fake(stored), Filter.Eq(Field.stored[BsonDocument, Int]("n").path, BsonInt32(5))) shouldBe 1
      found(fake(stored), Filter.Eq(Field.stored[BsonDocument, Double]("n").path, BsonDouble(5.0))) shouldBe 1
    }
  }

  "distinct over a field typed as an array" should {
    "answer with the elements' type, since the server answers with the elements" in {
      val values: List[String] = fake(BsonDocument("tags", arr("a", "b")), BsonDocument("tags", arr("b", "c"))).distinct(tagsF).all.unsafeRunSync()

      values shouldBe List("a", "b", "c")
    }
  }

  "a stream from the fake" should {
    "read the collection when it runs, not when it is built, as a cursor would" in {
      val c      = fake()
      val stream = c.find().stream

      c.insertOne(BsonDocument("name", BsonString("late"))).unsafeRunSync()

      stream.compile.toList.unsafeRunSync().map(_.getString("name").getValue) shouldBe List("late")
    }

    "stream an aggregation, distinct values and a selection, not only a find" in {
      val c = fake(BsonDocument("name", BsonString("a")).append("age", BsonInt32(1)), BsonDocument("name", BsonString("b")).append("age", BsonInt32(2)))

      c.aggregate[BsonDocument](Seq(Stage.sortBy(Sort.desc(ageF)))).stream.compile.toList.unsafeRunSync().map(_.getString("name").getValue) shouldBe
        List("b", "a")
      c.distinct(nameF).stream.compile.toList.unsafeRunSync() shouldBe List("a", "b")
      c.distinct(nameF).attempting.stream.compile.toList.unsafeRunSync() shouldBe List(Right("a"), Right("b"))
    }
  }

  "a refused stage" should {
    "still be named when its AST case carries no operator of its own" in {
      val c       = fake(BsonDocument("name", BsonString("a")))
      val refused = intercept[UnsupportedOperationException] {
        c.aggregate[BsonDocument](Seq(Stage.raw[BsonDocument](BsonDocument("$sample", BsonDocument())))).all.unsafeRunSync()
      }

      refused.getMessage should not include "FakeMongoCollection:  is"
      refused.getMessage.toLowerCase should include("raw")
    }
  }

  "inserting" should {
    "stamp an _id the way the driver does, and report it" in {
      val c      = fake()
      val result = c.insertOne(BsonDocument("name", BsonString("a"))).unsafeRunSync()

      result.insertedId.map(_.isObjectId) shouldBe Some(true)
      c.find().all.unsafeRunSync().head.containsKey("_id") shouldBe true
    }

    "keep an _id the caller supplied" in {
      val id = ObjectId.get()
      val c  = fake()
      c.insertOne(BsonDocument("_id", BsonObjectId(id)).append("name", BsonString("a"))).unsafeRunSync()

      c.find().all.unsafeRunSync().head.getObjectId("_id").getValue shouldBe id
    }

    "refuse a duplicate _id as a DuplicateKey, rather than storing it twice" in {
      val id  = ObjectId.get()
      val doc = BsonDocument("_id", BsonObjectId(id)).append("name", BsonString("a"))
      val c   = fake(doc)

      intercept[mongo4s.MongoError.DuplicateKey](c.insertOne(doc.clone()).unsafeRunSync())
      c.find().all.unsafeRunSync().size shouldBe 1
    }

    "refuse a duplicate _id within one insertMany, keeping what came before it, as an ordered insert does" in {
      val c      = fake()
      val failed = intercept[mongo4s.MongoError.BulkWriteFailed] {
        c.insertMany(List(keyed("a"), keyed("a"), keyed("b"))).unsafeRunSync()
      }

      failed.failures.map(_.index) shouldBe List(1)
      failed.result.insertedCount shouldBe 1
      c.count().unsafeRunSync() shouldBe 1
    }
  }

  "a replacement" should {
    "keep the _id of the document it replaces when it carries none of its own" in {
      val c = fake(keyed("a").append("name", BsonString("old")))

      c.replaceOne(nameF.equalTo("old"), BsonDocument("name", BsonString("new"))).unsafeRunSync()

      c.find().all.unsafeRunSync() shouldBe List(keyed("a").append("name", BsonString("new")))
    }

    "refuse to change the _id of the document it replaces" in {
      val c = fake(keyed("a").append("name", BsonString("old")))

      intercept[mongo4s.MongoError.Failed] {
        c.replaceOne(nameF.equalTo("old"), keyed("b").append("name", BsonString("new"))).unsafeRunSync()
      }.code shouldBe 66
    }
  }

  "an update that changes one path twice" should {
    "be refused as rendering refuses it, not applied one change after the other" in {
      val c = fake(keyed("a").append("n", BsonInt32(0)))
      val n = Field.stored[BsonDocument, Int]("n")

      intercept[IllegalArgumentException](c.updateOne(Filter.all, Update.inc(n, 1).and(Update.inc(n, 2))).unsafeRunSync())
      c.find().first.unsafeRunSync().map(_.getInt32("n").getValue) shouldBe Some(0)
    }
  }

  "an update that changes nothing" should {
    "count as matched but not modified" in {
      val c = fake(keyed("a").append("name", BsonString("same")))

      val result = c.updateOne(nameF.equalTo("same"), Update.set(nameF, "same")).unsafeRunSync()

      (result.matchedCount, result.modifiedCount) shouldBe (1L, 0L)
    }
  }

  "a collation" should {
    "be refused by name rather than ignored, since it changes what matches" in {
      val c         = fake(keyed("a"))
      val collation = com.mongodb.client.model.Collation.builder().locale("en").build()

      intercept[UnsupportedOperationException](c.find().collation(collation))
      intercept[UnsupportedOperationException](c.updateOne(Filter.all, Update.set(nameF, "x"), UpdateOptions.default.withCollation(collation)).unsafeRunSync())
      intercept[UnsupportedOperationException](c.deleteMany(Filter.all, DeleteOptions.default.withCollation(collation)).unsafeRunSync())
    }
  }

  "a unique index" should {
    val emailF = Field.stored[BsonDocument, String]("email")

    def withEmail(id: String, email: String): BsonDocument = keyed(id).append("email", BsonString(email))

    "refuse a second document with the same key, whether inserted or updated into it" in {
      val c = fake(withEmail("a", "x@y"), withEmail("b", "z@y"))
      c.createIndex(Index.ascending(emailF).withUnique).unsafeRunSync()

      intercept[mongo4s.MongoError.DuplicateKey](c.insertOne(withEmail("c", "x@y")).unsafeRunSync())
      intercept[mongo4s.MongoError.DuplicateKey](c.updateOne(emailF.equalTo("z@y"), Update.set(emailF, "x@y")).unsafeRunSync())
      c.count().unsafeRunSync() shouldBe 2
    }

    "let documents without the field through when it is sparse, and police only what its partial filter selects" in {
      val sparse  = fake(keyed("a"))
      val partial = fake(withEmail("a", "x@y").append("active", BsonBoolean(false)))

      sparse.createIndex(Index.ascending(emailF).withUnique.withSparse).unsafeRunSync()
      sparse.insertOne(keyed("b")).unsafeRunSync()

      partial.createIndex(Index.ascending(emailF).withUnique.where(Field.stored[BsonDocument, Boolean]("active").equalTo(true))).unsafeRunSync()
      partial.insertOne(withEmail("b", "x@y").append("active", BsonBoolean(true))).unsafeRunSync()

      (sparse.count().unsafeRunSync(), partial.count().unsafeRunSync()) shouldBe (2L, 2L)
    }

    "refuse to be created over documents that already break it" in {
      val c = fake(withEmail("a", "x@y"), withEmail("b", "x@y"))

      intercept[mongo4s.MongoError.DuplicateKey](c.createIndex(Index.ascending(emailF).withUnique).unsafeRunSync())
    }

    "be named the way the server names it, so it can be dropped by that name" in {
      val c    = fake(withEmail("a", "x@y"))
      val name = c.createIndex(Index.ascending(emailF).withUnique).unsafeRunSync()

      name shouldBe "email_1"
      c.dropIndex(name).unsafeRunSync()
      c.insertOne(withEmail("b", "x@y")).unsafeRunSync()
      c.count().unsafeRunSync() shouldBe 2
    }
  }

  "a bulk write that meets a duplicate _id" should {
    "stop there when ordered, and report the command by its position" in {
      val c      = fake(keyed("a"))
      val failed = intercept[mongo4s.MongoError.BulkWriteFailed] {
        c.bulkWrite(Seq(WriteCommand.InsertOne(keyed("b")), WriteCommand.InsertOne(keyed("a")), WriteCommand.InsertOne(keyed("c")))).unsafeRunSync()
      }

      failed.failures.map(_.index) shouldBe List(1)
      failed.result.insertedCount shouldBe 1
      c.count().unsafeRunSync() shouldBe 2
    }

    "carry on past it when unordered, and report every failure" in {
      val c      = fake(keyed("a"))
      val failed = intercept[mongo4s.MongoError.BulkWriteFailed] {
        c.bulkWrite(Seq(WriteCommand.InsertOne(keyed("a")), WriteCommand.InsertOne(keyed("b")), WriteCommand.InsertOne(keyed("a"))), ordered = false).unsafeRunSync()
      }

      failed.failures.map(_.index) shouldBe List(0, 2)
      failed.result.insertedCount shouldBe 1
      c.count().unsafeRunSync() shouldBe 2
    }
  }

  "a bulk update" should {
    "refuse the options it cannot honour, rather than dropping them" in {
      val c = fake(BsonDocument("name", BsonString("a")))

      intercept[UnsupportedOperationException] {
        c.bulkWrite(
          Seq(WriteCommand.UpdateOne(nameF.equalTo("a"), Update.set(ageF, 1), UpdateOptions.default.withUpsert))
        ).unsafeRunSync()
      }
    }
  }

  "$first" should {
    "take the first document's value even when that document lacks the field" in {
      val c       = fake(
        BsonDocument("g", BsonString("x")),
        BsonDocument("g", BsonString("x")).append("v", BsonInt32(5)),
      )
      val grouped = c
        .aggregate[BsonDocument](
          Seq(Stage.groupBy(Field.stored[BsonDocument, String]("g"))("first" -> Accumulator.first(Field.stored[BsonDocument, Int]("v"))))
        )
        .all
        .unsafeRunSync()

      grouped.head.get("first").isNull shouldBe true
    }
  }
