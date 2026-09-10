package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import org.bson.{BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.testkit.FakeMongoCollection

import cats.effect.unsafe.implicits.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object SelectSpec:
  final case class Person(id: String, name: String, age: Int)

  final case class Contact(id: String, nick: Option[String])

  object Contact:
    given BsonDocumentCodec[Contact] = BsonDocumentCodec.make(
      contact =>
        val document = BsonDocument().append("id", BsonString(contact.id))
        contact.nick.foreach(value => document.append("nick", BsonString(value)))
        document
      ,
      document =>
        Option(document.get("id"))
          .toRight(BsonError.MissingField("id"))
          .flatMap(BsonDecoder[String].decode)
          .map(id => Contact(id, Option(document.get("nick")).flatMap(BsonDecoder[String].decode(_).toOption))),
    )

  object Person:
    given BsonDocumentCodec[Person] = BsonDocumentCodec.make(
      person =>
        BsonDocument()
          .append("id", BsonString(person.id))
          .append("name", BsonString(person.name))
          .append("age", BsonInt32(person.age)),
      document =>
        for
          id   <- field[String](document, "id")
          name <- field[String](document, "name")
          age  <- field[Int](document, "age")
        yield Person(id, name, age),
    )

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

final class SelectSpec extends AnyWordSpec, Matchers:
  import SelectSpec.Person
  import SelectSpec.Person.given

  type S[A] = CatsStream[IO][A]

  private def collectionOf: FakeMongoCollection[IO, S, Person] =
    val collection = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]], _ => fs2.Stream.empty)
    collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))).unsafeRunSync()
    collection

  private def seeded = collectionOf.find()

  "selectAs" should {

    "decode into the named tuple it was asked for" in {
      val picked = seeded.selectAs[(name: String, age: Int)].all.unsafeRunSync()

      picked shouldBe List((name = "bob", age = 30), (name = "alice", age = 25))
    }

    "read a field by its label" in {
      val first = seeded.selectAs[(name: String, age: Int)].first.unsafeRunSync()

      first.map(_.name) shouldBe Some("bob")
      first.map(_.age) shouldBe Some(30)
    }

    "follow the order the shape declares" in {
      val picked = seeded.selectAs[(age: Int, name: String)].all.unsafeRunSync()

      picked.head.age shouldBe 30
      picked.head.name shouldBe "bob"
    }

    "apply the filter it was built from" in {
      val nameField = mongo4s.Field.of[Person, String](_.name)
      val picked    = collectionOf.find(nameField.equalTo("alice")).selectAs[(age: Int, name: String)].all.unsafeRunSync()

      picked.map(_.age) shouldBe List(25)
    }

    "report a document missing a selected field, rather than guessing" in {
      val collection = FakeMongoCollection[IO, S, Person](summon[BsonDocumentCodec[Person]], _ => fs2.Stream.empty)
      collection.insertRaw(BsonDocument("id", BsonString("3")).append("name", BsonString("carol")))

      val attempts = collection.find().selectAs[(name: String, age: Int)].attempting.all.unsafeRunSync()

      attempts.head.isLeft shouldBe true
    }

    "refuse a label that is not a field of the entity" in {
      "seeded.selectAs[(nickname: String)]" shouldNot typeCheck
    }

    "refuse a field asked for at the wrong type" in {
      "seeded.selectAs[(age: String)]" shouldNot typeCheck
    }
  }

  "selectAs over an optional field" should {

    def contacts: FakeMongoCollection[IO, S, SelectSpec.Contact] =
      import SelectSpec.Contact.given
      val collection = FakeMongoCollection[IO, S, SelectSpec.Contact](summon[BsonDocumentCodec[SelectSpec.Contact]], _ => fs2.Stream.empty)
      collection.insertMany(List(SelectSpec.Contact("1", None), SelectSpec.Contact("2", Some("bo")))).unsafeRunSync()
      collection

    "read an omitted field as None, the same answer find gives for the same document" in {
      import SelectSpec.Contact.given
      val whole    = contacts.find().all.unsafeRunSync().map(_.nick)
      val selected = contacts.find().selectAs[(id: String, nick: Option[String])].all.unsafeRunSync().map(_.nick)

      whole shouldBe List(None, Some("bo"))
      selected shouldBe whole
    }

    "still report a field the entity requires but the document does not carry" in {
      val collection = collectionOf
      collection.insertRaw(BsonDocument().append("id", BsonString("3")).append("name", BsonString("eve")))

      val failures = collection.find().selectAs[(name: String, age: Int)].attempting.all.unsafeRunSync()

      failures.last.left.map(_.message) shouldBe Left("Missing field: age")
    }
  }
