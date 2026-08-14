package mongo4s.repositories

import mongo4s.bson.*
import mongo4s.bson.BsonInstances.given
import mongo4s.operations.{Update, WriteCommand}
import mongo4s.{Effect, Field, PrimaryKey, Streamable}
import org.bson.{BsonDocument, BsonInt32, BsonString}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object RepositoryBackendSpec:

  final case class Person(id: String, name: String, age: Int)

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

    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

    private def field[A: BsonDecoder](document: BsonDocument, name: String): Either[BsonError, A] =
      Option(document.get(name)).toRight(BsonError.MissingField(name)).flatMap(BsonDecoder[A].decode)

// Same battery of repository behaviors run against every mongo4s runtime backend, against a pure
// in-memory FakeMongoCollection (from mongo4s-repositories' own test sources, reused here). `run`/`drain`
// bottom out in each backend's own blocking-run primitive - matching the pattern already used by
// the per-runtime RsBridge specs (CatsBridgeSpec/ZioBridgeSpec/RapidBridgeSpec/KyoBridgeSpec) and
// mongo4s.benchmarks.RuntimeBenchmark - rather than cats-effect-testing-scalatest's AsyncIOSpec,
// which only exists for cats.effect.IO and can't generalize across F[*].
trait RepositoryBackendSpec[F[*], S[*]] extends AnyWordSpec, Matchers:
  import RepositoryBackendSpec.Person
  import RepositoryBackendSpec.Person.given

  protected def effectInstance: Effect[F]

  protected def run[A](fa: F[A]): A

  // Fixed to `Person`, not generic `[A]` - every call site in this spec only ever needs `Person`,
  // and keeping these concrete lets the kyo backend derive its `Tag[Emit[Chunk[Person]]]`/
  // `Tag[Poll[Chunk[Person]]]` directly in the override body (a generic `[A]` override, compiled
  // once, couldn't - same root constraint as `RsBridge.stream`, see `mongo4s.Streamable`).
  protected def drain(stream: S[Person]): List[Person]
  protected def emitStream(values: List[Person]): S[Person]

  /** Evidence that `S[Person]` can actually be streamed - see `mongo4s.Streamable`. Every backend
    * satisfies this for a concrete entity type like `Person`; kept abstract (rather than a trait-wide
    * `given`) for the same reason `effectInstance` is - to avoid the given resolving back to itself.
    */
  protected def streamable: Streamable[S, Person]

  protected def supportsStreaming: Boolean = true

  // `given` is scoped to this method only, never to the trait itself: a trait-wide `given Effect[F]
  // = effectInstance` would itself be a candidate implicit at the point subclasses implement
  // `effectInstance` (typically via `summon[Effect[ConcreteF]]`), which can resolve back to itself
  // and deadlock in a circular lazy-val initialization instead of finding the real backend instance.
  private def repo(batchSize: Int = 500): (FakeMongoCollection[F, S, Person], BaseMongoRepository[F, S, Person, String]) =
    given Effect[F]               = effectInstance
    val collection                = FakeMongoCollection[F, S, Person](summon[BsonDocumentCodec[Person]], emitStream)
    (collection, new BaseMongoRepository(collection, batchSize))

  "findOne" should {
    "return the entity matching the key" in {
      val (collection, repository) = repo()
      run(collection.insertOne(Person("1", "bob", 30)))

      run(repository.findOne("1")) shouldBe Some(Person("1", "bob", 30))
      run(repository.findOne("missing")) shouldBe None
    }
  }

  "findMany" should {
    "batch lookups across multiple round trips" in {
      val (collection, repository) = repo(batchSize = 2)
      run(collection.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3))))

      run(repository.findMany(List("1", "2", "3"))) should contain theSameElementsAs
        List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3))
    }
  }

  "findBy / findByFilter" should {
    "filter by a single field" in {
      val (collection, repository) = repo()
      run(collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))

      run(repository.findBy(Field.of[Person, String](_.name), "bob")) shouldBe List(Person("1", "bob", 30))
    }

    "filter by an arbitrary Filter" in {
      val (collection, repository) = repo()
      val filter                   = Field.of[Person, Int](_.age).gte(28)
      run(collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))

      run(repository.findByFilter(filter)) shouldBe List(Person("1", "bob", 30))
    }
  }

  if supportsStreaming then
    "getAll / getBy" should {
      "stream every document, and filtered documents" in {
        given Streamable[S, Person]  = streamable
        val (collection, repository) = repo()
        run(collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))

        drain(repository.getAll) should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25))

        val filter = Field.of[Person, String](_.name).equalTo("alice")
        drain(repository.getBy(filter)) shouldBe List(Person("2", "alice", 25))
      }
    }

  "insertOne / insertMany" should {
    "add documents, batching inserts across round trips" in {
      val (collection, repository) = repo(batchSize = 2)
      run(repository.insertOne(Person("1", "bob", 30)))
      run(repository.insertMany(List(Person("2", "alice", 25), Person("3", "eve", 40), Person("4", "carl", 22))))

      collection.snapshot should contain theSameElementsAs
        List(Person("1", "bob", 30), Person("2", "alice", 25), Person("3", "eve", 40), Person("4", "carl", 22))
    }
  }

  "upsert / upsertMany" should {
    "insert when the key is new and replace when it already exists" in {
      val (collection, repository) = repo()
      run(repository.upsert(Person("1", "bob", 30)))
      run(repository.upsert(Person("1", "bob", 31)))

      collection.snapshot shouldBe List(Person("1", "bob", 31))
    }

    "batch upserts across round trips" in {
      val (collection, repository) = repo(batchSize = 1)
      run(repository.upsertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))

      collection.snapshot should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25))
    }
  }

  "updateField" should {
    "set a single field on the entity matching the key" in {
      val (collection, repository) = repo()
      run(collection.insertOne(Person("1", "bob", 30)))

      run(repository.updateField("1", Field.of[Person, Int](_.age), 31))

      collection.snapshot shouldBe List(Person("1", "bob", 31))
    }
  }

  "updateBy" should {
    "apply an update to every document matching the filter and report the modified count" in {
      val (collection, repository) = repo()
      run(collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 30), Person("3", "eve", 40))))

      val modified = run(repository.updateBy(Field.of[Person, Int](_.age).equalTo(30), Update.set(Field.of[Person, Int](_.age), 99)))

      modified shouldBe 2L
      collection.snapshot should contain theSameElementsAs List(Person("1", "bob", 99), Person("2", "alice", 99), Person("3", "eve", 40))
    }
  }

  "bulkWrite" should {
    "apply mixed write commands, batching across round trips" in {
      val (collection, repository) = repo(batchSize = 1)
      run(collection.insertOne(Person("1", "bob", 30)))

      run(
        repository.bulkWrite(
          Seq(
            WriteCommand.InsertOne(Person("2", "alice", 25)),
            WriteCommand.DeleteOne(Field.of[Person, String](_.id).equalTo("1")),
          )
        )
      )

      collection.snapshot shouldBe List(Person("2", "alice", 25))
    }
  }

  "deleteOne / deleteMany" should {
    "remove by key, batching deletes across round trips" in {
      val (collection, repository) = repo(batchSize = 1)
      run(collection.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3))))

      run(repository.deleteOne("1"))
      run(repository.deleteMany(List("2", "3")))

      collection.snapshot shouldBe empty
    }
  }

  "count" should {
    "report the total and filtered document count" in {
      val (collection, repository) = repo()
      run(collection.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))

      run(repository.count) shouldBe 2L
      run(repository.count(Field.of[Person, Int](_.age).gt(26))) shouldBe 1L
    }
  }
