package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import zio.schema.codec.BsonSchemaCodec
import zio.schema.{DeriveSchema, Schema}

import mongo4s.cats.CatsStream
import mongo4s.{Field, PrimaryKey}
import mongo4s.bson.BsonDocumentCodec
import mongo4s.testkit.FakeMongoCollection
import mongo4s.bson.medeia.MedeiaDocumentCodec
import mongo4s.bson.calypso.{CalypsoDecoder, CalypsoEncoder}
import mongo4s.bson.ziobson.{ZioBsonDecoder, ZioBsonEncoder}

import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object CodecRepositorySpec:

  final case class Person(id: String, name: String, age: Int) derives MedeiaDocumentCodec

  given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

  given CalypsoEncoder[Person] =
    CalypsoEncoder.forProduct3("id", "name", "age")(p => (p.id, p.name, p.age))
  given CalypsoDecoder[Person] =
    CalypsoDecoder.forProduct3("id", "name", "age")((id, name, age) => Person(id, name, age))

  given Schema[Person] = DeriveSchema.gen[Person]

  def medeiaCodec: BsonDocumentCodec[Person] =
    import mongo4s.bson.medeia.MedeiaInstances.given
    summon[BsonDocumentCodec[Person]]

  def calypsoCodec: BsonDocumentCodec[Person] =
    import mongo4s.bson.calypso.CalypsoInstances.given
    summon[BsonDocumentCodec[Person]]

  def zioBsonCodec: BsonDocumentCodec[Person] =
    given ZioBsonEncoder[Person] = BsonSchemaCodec.bsonEncoder(summon[Schema[Person]])
    given ZioBsonDecoder[Person] = BsonSchemaCodec.bsonDecoder(summon[Schema[Person]])
    import mongo4s.bson.ziobson.ZioBsonInstances.given
    summon[BsonDocumentCodec[Person]]

final class CodecRepositorySpec extends AnyWordSpec, Matchers:
  import CodecRepositorySpec.*

  type S[A] = CatsStream[IO][A]

  private def repoFor(codec: BsonDocumentCodec[Person]): (FakeMongoCollection[IO, S, Person], BaseMongoRepository[IO, S, Person, String]) =
    val collection = FakeMongoCollection[IO, S, Person](codec, list => fs2.Stream.emits(list).covary[IO])
    (collection, BaseMongoRepository(collection))

  List("medeia" -> medeiaCodec, "calypso" -> calypsoCodec, "zio-bson" -> zioBsonCodec).foreach { (codecName, codec) =>
    s"BaseMongoRepository backed by the $codecName codec" should {

      "round-trip an entity through insertOne/findOne" in {
        val (_, repository) = repoFor(codec)
        repository.insertOne(Person("1", "bob", 30)).unsafeRunSync()

        repository.findOne("1").unsafeRunSync() shouldBe Some(Person("1", "bob", 30))
      }

      "apply a field-level update correctly" in {
        val (collection, repository) = repoFor(codec)
        collection.insertOne(Person("1", "bob", 30)).unsafeRunSync()

        repository.updateField("1", Field.of[Person, Int](_.age), 31).unsafeRunSync()

        collection.snapshot shouldBe List(Person("1", "bob", 31))
      }

      "filter correctly via findByFilter" in {
        val (collection, repository) = repoFor(codec)
        collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))).unsafeRunSync()

        repository.findByFilter(Field.of[Person, String](_.name).equalTo("alice")).unsafeRunSync() shouldBe List(Person("2", "alice", 25))
      }
    }
  }
