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

final class FakeFidelitySpec extends AnyWordSpec, Matchers:
  import FakeFidelitySpec.*

  type S[A] = CatsStream[IO][A]

  private def fake(seed: BsonDocument*): FakeMongoCollection[IO, S, BsonDocument] =
    val c = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]], _ => fs2.Stream.empty)
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
