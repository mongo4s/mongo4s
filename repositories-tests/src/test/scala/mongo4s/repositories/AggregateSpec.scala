package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.{Accumulator, Filter, Sort, Stage}
import mongo4s.testkit.FakeMongoCollection
import mongo4s.Field

import cats.effect.unsafe.implicits.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object AggregateSpec:
  final case class Person(id: String, name: String, age: Int, city: String)

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person =>
        BsonDocument()
          .append("id", BsonString(person.id))
          .append("name", BsonString(person.name))
          .append("age", BsonInt32(person.age))
          .append("city", BsonString(person.city)),
      document =>
        for
          id   <- field[String](document, "id")
          name <- field[String](document, "name")
          age  <- field[Int](document, "age")
          city <- field[String](document, "city")
        yield Person(id, name, age, city),
    )

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

  val people: List[Person] = List(
    Person("1", "bob", 30, "berlin"),
    Person("2", "alice", 25, "berlin"),
    Person("3", "carol", 41, "prague"),
    Person("4", "dave", 30, "prague"),
  )

final class AggregateSpec extends AnyWordSpec, Matchers:
  import AggregateSpec.*
  import AggregateSpec.Person.given

  type S[A] = CatsStream[IO][A]

  private val age  = Field.of[Person, Int](_.age)
  private val city = Field.of[Person, String](_.city)
  private val name = Field.of[Person, String](_.name)

  private def collection: FakeMongoCollection[IO, S, Person] =
    val fake = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]], _ => fs2.Stream.empty)
    fake.insertMany(people).unsafeRunSync()
    fake

  private def run(pipeline: Stage[Person]*): List[BsonDocument] =
    collection.aggregate[BsonDocument](pipeline).all.unsafeRunSync()

  "the fake's aggregate" should {

    "filter, sort, skip and limit in pipeline order" in {
      val stages = Seq(
        Stage.matching(age.gte(30)),
        Stage.sortBy(Sort.desc(age)),
        Stage.skip(1),
        Stage.limit(1),
      )

      collection.aggregate[Person](stages).all.unsafeRunSync() shouldBe List(Person("1", "bob", 30, "berlin"))
    }

    "count the documents that reached the stage, as an Int32" in {
      run(Stage.matching(city.equalTo("berlin")), Stage.count("total")) shouldBe
        List(BsonDocument("total", BsonInt32(2)))
    }

    "group by a field, keeping the order the keys were first seen in" in {
      val grouped = run(Stage.groupBy(city)("people" -> Accumulator.count[Person]))

      grouped shouldBe List(
        BsonDocument("_id", BsonString("berlin")).append("people", BsonInt32(2)),
        BsonDocument("_id", BsonString("prague")).append("people", BsonInt32(2)),
      )
    }

    "group without a key into a single null-keyed document" in {
      run(Stage.groupAll("oldest" -> Accumulator.max(age))).head.get("_id").isNull shouldBe true
    }

    "sum as an Int32 while the operands and the total are Int32" in {
      run(Stage.groupBy(city)("years" -> Accumulator.sum(age))).map(_.get("years")) shouldBe
        List(BsonInt32(55), BsonInt32(71))
    }

    "average as a Double, the way MongoDB does" in {
      run(Stage.groupBy(city)("mean" -> Accumulator.avg(age))).map(_.get("mean")) shouldBe
        List(BsonDouble(27.5), BsonDouble(35.5))
    }

    "carry min, max, first, last and push through a group" in {
      val grouped = run(
        Stage.matching(city.equalTo("berlin")),
        Stage.groupBy(city)(
          "min"   -> Accumulator.min(age),
          "max"   -> Accumulator.max(age),
          "first" -> Accumulator.first(name),
          "last"  -> Accumulator.last(name),
          "all"   -> Accumulator.push(name),
        ),
      ).head

      grouped.get("min") shouldBe BsonInt32(25)
      grouped.get("max") shouldBe BsonInt32(30)
      grouped.get("first") shouldBe BsonString("bob")
      grouped.get("last") shouldBe BsonString("alice")
      grouped.get("all") shouldBe BsonArray(java.util.List.of(BsonString("bob"), BsonString("alice")))
    }

    "decode the output into whatever codec the call asked for" in {
      collection
        .aggregate[Person](Seq(Stage.matching(Filter.all[Person]), Stage.sortBy(Sort.asc(name)), Stage.limit(1)))
        .all
        .unsafeRunSync() shouldBe List(Person("2", "alice", 25, "berlin"))
    }
  }

  "the fake's aggregate" should {

    "refuse a stage it does not simulate, naming it" in {
      val refused = intercept[UnsupportedOperationException] {
        run(Stage.unwind(Field.of[Person, String](_.city)))
      }

      refused.getMessage should include("$unwind")
    }

    "refuse $addToSet rather than invent an order for it" in {
      val refused = intercept[UnsupportedOperationException] {
        run(Stage.groupBy(city)("names" -> Accumulator.addToSet(name)))
      }

      refused.getMessage should include("$addToSet")
    }

    "refuse a raw accumulator" in {
      intercept[UnsupportedOperationException] {
        run(Stage.groupBy(city)("raw" -> Accumulator.raw[Person](BsonDocument("$stdDevPop", BsonString("$age")))))
      }
    }
  }

  "aggregate's output type" should {

    "need only a decoder, since nothing is ever encoded into it" in {
      final case class Counted(total: Int)

      given BsonDocumentDecoder[Counted] = document =>
        Option(document.get("total"))
          .toRight(BsonError.MissingField("total"))
          .flatMap(BsonDecoder[Int].decode)
          .map(Counted.apply)

      collection.aggregate[Counted](Seq(Stage.count("total"))).all.unsafeRunSync() shouldBe List(Counted(4))
    }
  }
