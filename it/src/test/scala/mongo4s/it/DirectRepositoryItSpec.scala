package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.MongoDBContainer

import mongo4s.bson.direct.WireCodec
import mongo4s.repositories.BaseMongoRepository
import mongo4s.operations.{Update, WriteCommand}
import mongo4s.{Effect, Field, MongoClient, PrimaryKey, RsBridge, Streamable}

import mongo4s.bson.BsonInstances.given

object DirectRepositoryItSpec:

  final case class Person(id: String, name: String, age: Int) derives WireCodec

  object Person:
    given PrimaryKey[Person, String] = PrimaryKey.single("id")(_.id)

trait DirectRepositoryItSpec[F[*], S[*]] extends AnyWordSpec, Matchers, BeforeAndAfterAll:
  import DirectRepositoryItSpec.Person
  import DirectRepositoryItSpec.Person.given

  protected def effectInstance: Effect[F]
  protected def rsBridge: RsBridge[F, S]
  protected def streamable: Streamable[S, Person]

  protected def run[A](fa: F[A]): A
  protected def drain(stream: S[Person]): List[Person]

  protected val container: MongoDBContainer = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  private def repository(dbName: String): (MongoClient[F, S], BaseMongoRepository[F, S, Person, String]) =
    given Effect[F]      = effectInstance
    given RsBridge[F, S] = rsBridge
    val client           = run(MongoClient.fromConnectionString[F, S](container.getConnectionString))
    val database         = run(client.getDatabase(dbName))
    val collection       = run(database.getDirectCollection[Person]("people"))
    (client, BaseMongoRepository(collection))

  "BaseMongoRepository backed by getDirectCollection (native WireCodec, no BsonDocument) against a real MongoDB" should {

    "insertOne / findOne round-trip an entity" in {
      val (client, repo) = repository("direct_it_find_one")
      run(repo.insertOne(Person("1", "bob", 30)))
      val found          = run(repo.findOne("1"))
      val missing        = run(repo.findOne("missing"))
      run(client.close)

      found shouldBe Some(Person("1", "bob", 30))
      missing shouldBe None
    }

    "insertMany / findMany / findBy / findByFilter batch and filter correctly" in {
      val (client, repo) = repository("direct_it_find_many")
      run(repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25), Person("3", "eve", 40))))
      val many           = run(repo.findMany(List("1", "3")))
      val byName         = run(repo.findBy(Field.of[Person, String](_.name), "alice"))
      val byFilter       = run(repo.findByFilter(Field.of[Person, Int](_.age).gt(28)))
      run(client.close)

      many should contain theSameElementsAs List(Person("1", "bob", 30), Person("3", "eve", 40))
      byName shouldBe List(Person("2", "alice", 25))
      byFilter.map(_.id) should contain allOf ("1", "3")
    }

    "getAll / getBy stream matching documents" in {
      given Streamable[S, Person] = streamable
      val (client, repo)          = repository("direct_it_streaming")
      run(repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 25))))
      val all                     = drain(repo.getAll)
      val filtered                = drain(repo.getBy(Field.of[Person, String](_.name).equalTo("alice")))
      run(client.close)

      all should contain theSameElementsAs List(Person("1", "bob", 30), Person("2", "alice", 25))
      filtered shouldBe List(Person("2", "alice", 25))
    }

    "upsert / upsertMany insert new keys and replace existing ones" in {
      val (client, repo) = repository("direct_it_upsert")
      run(repo.upsert(Person("1", "bob", 30)))
      run(repo.upsert(Person("1", "bob", 31)))
      run(repo.upsertMany(List(Person("2", "alice", 25))))
      val found          = run(repo.findOne("1"))
      val found2         = run(repo.findOne("2"))
      run(client.close)

      found shouldBe Some(Person("1", "bob", 31))
      found2 shouldBe Some(Person("2", "alice", 25))
    }

    "updateField / updateBy apply real Mongo update operators" in {
      val (client, repo) = repository("direct_it_update")
      run(repo.insertMany(List(Person("1", "bob", 30), Person("2", "alice", 30), Person("3", "eve", 40))))
      run(repo.updateField("1", Field.of[Person, Int](_.age), 99))
      val modified       = run(repo.updateBy(Field.of[Person, Int](_.age).equalTo(30), Update.set(Field.of[Person, Int](_.age), 50)))
      val one            = run(repo.findOne("1"))
      val two            = run(repo.findOne("2"))
      run(client.close)

      // "1" was already moved off age=30 by updateField, so updateBy's age==30 filter only still matches "2"
      modified shouldBe 1L
      one shouldBe Some(Person("1", "bob", 99))
      two shouldBe Some(Person("2", "alice", 50))
    }

    "bulkWrite applies mixed write commands" in {
      given Streamable[S, Person] = streamable
      val (client, repo)          = repository("direct_it_bulk")
      run(repo.insertOne(Person("1", "bob", 30)))
      run(
        repo.bulkWrite(
          Seq(
            WriteCommand.InsertOne(Person("2", "alice", 25)),
            WriteCommand.DeleteOne(Field.of[Person, String](_.id).equalTo("1")),
          )
        )
      )
      val all                     = drain(repo.getAll)
      run(client.close)

      all shouldBe List(Person("2", "alice", 25))
    }

    "deleteOne / deleteMany / count remove and report correctly" in {
      val (client, repo) = repository("direct_it_delete")
      run(repo.insertMany(List(Person("1", "a", 1), Person("2", "b", 2), Person("3", "c", 3))))
      run(repo.deleteOne("1"))
      run(repo.deleteMany(List("2", "3")))
      val remaining      = run(repo.count())
      run(client.close)

      remaining shouldBe 0L
    }
  }
