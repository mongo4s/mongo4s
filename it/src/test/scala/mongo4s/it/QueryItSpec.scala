package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString}

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.{Field, MongoClient, MongoCollection, RsBridge}
import mongo4s.operations.*

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object QueryItSpec:

  final case class Bucket(id: Int, count: Int)

  object Bucket:
    given BsonDocumentCodec[Bucket] = BsonDocumentCodec.make(
      bucket => BsonDocument().append("_id", BsonInt32(bucket.id)).append("count", BsonInt32(bucket.count)),
      document =>
        for
          id    <- BsonDecoder[Int].decode(document.get("_id"))
          count <- BsonDecoder[Int].decode(document.get("count"))
        yield Bucket(id, count),
    )

final class QueryItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import CoreItSpec.Person
  import QueryItSpec.Bucket

  private val container = new MongoDBContainer("mongo:8.2").withReplicaSet()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val nameField = Field.of[Person, String](_.name)
  private val ageField  = Field.of[Person, Int](_.age)

  private val people = List(Person("bob", 30), Person("alice", 25), Person("carol", 30), Person("dan", 17))

  private def seeded(name: String): IO[MongoCollection[IO, S, Person]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("query_it")
      collection <- database.getCollection[Person](name)
      _          <- collection.insertMany(people)
    yield collection

  "aggregate" should {

    "run a $group with typed accumulators" in {
      val program =
        for
          collection <- seeded("group")
          buckets    <- collection
                          .aggregate[Bucket](
                            Seq(
                              Stage.matching(ageField.gte(18)),
                              Stage.groupBy(ageField)("count" -> Accumulator.count[Person]),
                              Stage.raw[Person](BsonDocument("$sort", BsonDocument("_id", BsonInt32(1)))),
                            )
                          )
                          .all
        yield buckets

      program.asserting(_ shouldBe List(Bucket(25, 1), Bucket(30, 2)))
    }

    "run a $facet, which the server rejects if the sub-pipelines are malformed" in {
      val program =
        for
          collection <- seeded("facet")
          result     <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.facet[Person](
                                "adults" -> List(Stage.matching(ageField.gte(18)), Stage.count("n")),
                                "minors" -> List(Stage.matching(ageField.lt(18)), Stage.count("n")),
                              )
                            )
                          )
                          .first
        yield result.map(_.toJson)

      program.asserting(_ shouldBe Some("""{"adults": [{"n": 3}], "minors": [{"n": 1}]}"""))
    }

    "push $limit into the pipeline for first, rather than draining the cursor" in {
      seeded("first").flatMap(_.aggregate[Person](Seq(Stage.sortBy(Sort.asc(nameField)))).first).asserting(_ shouldBe Some(Person("alice", 25)))
    }

    "leave a $out pipeline alone in first, since the server rejects any stage after it" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("query_it")
          source   <- database.getCollection[Person]("aggregate_out_source")
          _        <- source.insertMany(people)
          _        <- source.aggregate[Person](Seq(Stage.matching(ageField.gte(18)), Stage.out("aggregate_out_target"))).first
          target   <- database.getCollection[Person]("aggregate_out_target")
          written  <- target.find(ageField.gte(0)).all
        yield written.map(_.name).sorted

      program.asserting(_ shouldBe List("alice", "bob", "carol"))
    }

    "run a $lookup driven by a sub-pipeline and a $graphLookup, both of which the server validates" in {
      val program =
        for
          collection <- seeded("lookup_pipeline")
          joined     <- collection
                          .aggregate[BsonDocument](
                            Seq(
                              Stage.matching(nameField.equalTo("bob")),
                              Stage.lookupWith[Person, Person](
                                from = "lookup_pipeline",
                                pipeline = List(Stage.matching(ageField.gte(30))),
                                as = "peers",
                              ),
                              Stage.graphLookup[Person, Person, String](
                                from = "lookup_pipeline",
                                startWith = BsonString("$name"),
                                connectFrom = nameField,
                                connectTo = nameField,
                                as = "chain",
                                options = GraphLookupOptions.default[Person].withMaxDepth(1),
                              ),
                            )
                          )
                          .first
        yield joined.map(document => (document.getArray("peers").size, document.getArray("chain").size))

      program.asserting(_ shouldBe Some((2, 1)))
    }

    "run a $merge that names its target database and keeps existing documents" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("query_it")
          source   <- database.getCollection[Person]("merge_source")
          _        <- source.insertMany(people)
          options   = MergeOptions.default
                        .inDatabase("query_it")
                        .onFields(List("_id"))
                        .whenMatched(MergeOptions.WhenMatched.KeepExisting)
                        .whenNotMatched(MergeOptions.WhenNotMatched.Insert)
          _        <- source.aggregate[Person](Seq(Stage.matching(ageField.gte(18)), Stage.merge("merge_target", options))).first
          target   <- database.getCollection[Person]("merge_target")
          written  <- target.find(ageField.gte(0)).all
        yield written.map(_.name).sorted

      program.asserting(_ shouldBe List("alice", "bob", "carol"))
    }

    "accept allowDiskUse, maxTime and comment" in {
      val program =
        for
          collection <- seeded("aggregate_options")
          all        <- collection
                          .aggregate[Person](Seq(Stage.matching(ageField.gte(18))))
                          .allowDiskUse(true)
                          .maxTime(10.seconds)
                          .comment("it-spec")
                          .batchSize(2)
                          .all
        yield all.size

      program.asserting(_ shouldBe 3)
    }

    "stream a pipeline" in {
      seeded("aggregate_stream")
        .flatMap(_.aggregate[Person](Seq(Stage.sortBy(Sort.asc(nameField)))).stream.compile.toList)
        .asserting(_.map(_.name) shouldBe List("alice", "bob", "carol", "dan"))
    }
  }

  "distinct" should {

    "return each value once, decoded through the field's own type" in {
      seeded("distinct").flatMap(_.distinct(ageField).all).asserting(_.sorted shouldBe List(17, 25, 30))
    }

    "narrow by a filter" in {
      seeded("distinct_filtered").flatMap(_.distinct(ageField, ageField.gte(18)).all).asserting(_.sorted shouldBe List(25, 30))
    }
  }

  "the find-and-modify trio" should {

    "return the updated document when asked for it, and the previous one otherwise" in {
      val program =
        for
          collection <- seeded("find_one_and_update")
          after      <- collection.findOneAndUpdate(nameField.equalTo("bob"), Update.inc(ageField, 1))
          before     <- collection.findOneAndUpdate(nameField.equalTo("bob"), Update.inc(ageField, 1), FindOneAndUpdateOptions.default[Person].returningPrevious)
          settled    <- collection.find(nameField.equalTo("bob")).first
        yield (after, before, settled)

      program.asserting { (after, before, settled) =>
        after shouldBe Some(Person("bob", 31))
        before shouldBe Some(Person("bob", 31))
        settled shouldBe Some(Person("bob", 32))
      }
    }

    "pick the document a sort selects" in {
      val program =
        for
          collection <- seeded("find_one_and_update_sorted")
          youngest   <- collection.findOneAndUpdate(ageField.gte(0), Update.set(nameField, "picked"), FindOneAndUpdateOptions.default[Person].withSort(Sort.asc(ageField)))
        yield youngest.map(_.name)

      program.asserting(_ shouldBe Some("picked"))
    }

    "upsert and report the inserted document" in {
      val program =
        for
          collection <- seeded("find_one_and_update_upsert")
          created    <- collection.findOneAndUpdate(nameField.equalTo("erin"), Update.set(ageField, 22), FindOneAndUpdateOptions.upsert[Person])
        yield created

      program.asserting(_ shouldBe Some(Person("erin", 22)))
    }

    "replace a document atomically" in {
      val program =
        for
          collection <- seeded("find_one_and_replace")
          replaced   <- collection.findOneAndReplace(nameField.equalTo("bob"), Person("bob", 99))
        yield replaced

      program.asserting(_ shouldBe Some(Person("bob", 99)))
    }

    "delete and return what it removed" in {
      val program =
        for
          collection <- seeded("find_one_and_delete")
          removed    <- collection.findOneAndDelete(nameField.equalTo("bob"))
          left       <- collection.count()
        yield (removed, left)

      program.asserting { (removed, left) =>
        removed shouldBe Some(Person("bob", 30))
        left shouldBe 3L
      }
    }

    "return None when nothing matches" in {
      seeded("find_one_and_delete_missing").flatMap(_.findOneAndDelete(nameField.equalTo("nobody"))).asserting(_ shouldBe None)
    }
  }

  "attempting" should {
    "surface an undecodable document as a Left instead of failing the query" in {
      val program =
        for
          client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database   <- client.getDatabase("query_it")
          collection <- database.getCollection[Person]("attempting")
          _          <- collection.insertOne(Person("bob", 30))
          _          <- RsBridge[IO, S].one(collection.underlying.insertOne(BsonDocument().append("name", BsonString("broken"))))
          attempts   <- collection.find().attempting.all
          strict     <- collection.find().all.attempt
        yield (attempts, strict)

      program.asserting { (attempts, strict) =>
        attempts.collect { case Right(person) => person } shouldBe List(Person("bob", 30))
        attempts.count(_.isLeft) shouldBe 1
        strict.isLeft shouldBe true
      }
    }
  }

  "query options" should {
    "reach the server without being rejected" in {
      val program =
        for
          collection <- seeded("find_options")
          found      <- collection
                          .find(ageField.gte(18))
                          .sort(Sort.desc(ageField))
                          .projection(Projection.empty[Person].include(nameField).include(ageField).withoutId)
                          .skip(1)
                          .limit(1)
                          .maxTime(10.seconds)
                          .comment("it-spec")
                          .batchSize(10)
                          .all
        yield found

      program.asserting(_.map(_.age) shouldBe List(30))
    }

    "carry let variables into a find and an aggregate, and the cursor knobs that change nothing in the result" in {
      val olderThan = Filter.expr[Person](BsonDocument("$gt", BsonArray(java.util.List.of(BsonString("$age"), BsonString("$$min")))))
      val variables = BsonDocument("min", BsonInt32(26))

      val program =
        for
          collection <- seeded("let_queries")
          found      <- collection.find(olderThan).let(variables).allowDiskUse(true).noCursorTimeout.sort(Sort.asc(nameField)).all
          aggregated <- collection.aggregate[Person](Seq(Stage.matching(olderThan), Stage.sortBy(Sort.asc(nameField)))).let(variables).all
          distinct   <- collection.distinct(nameField, Filter.all).comment("it-spec").hint(BsonDocument("_id", BsonInt32(1))).all
          estimated  <- collection.estimatedCount(EstimatedCountOptions.default.withMaxTime(5.seconds).withComment("it-spec"))
        yield (found.map(_.name), aggregated.map(_.name), distinct.size, estimated)

      program.asserting(_ shouldBe (List("bob", "carol"), List("bob", "carol"), 4, 4L))
    }

    "pass a hint to distinct, failing on an index that does not exist" in {
      val program =
        for
          collection <- seeded("distinct_hint")
          failed     <- collection.distinct(nameField, Filter.all).hint(BsonDocument("missing", BsonInt32(1))).all.attempt
        yield failed.isLeft

      program.asserting(_ shouldBe true)
    }

    "keep a tailable cursor open on a capped collection, delivering a write it was waiting for" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("query_it")
          _        <- database.createCollection("tailed", CreateCollectionOptions.default.withCapped(1024 * 1024))
          people   <- database.getCollection[Person]("tailed")
          _        <- people.insertMany(List(Person("first", 1), Person("second", 2)))
          seen     <- people
                        .find()
                        .cursorType(com.mongodb.CursorType.TailableAwait)
                        .maxAwaitTime(200.millis)
                        .stream
                        .zipWithIndex
                        .evalTap((_, i) => if i == 1 then people.insertOne(Person("third", 3)).void else IO.unit)
                        .map(_._1)
                        .take(3)
                        .compile
                        .toList
        yield seen.map(_.name)

      program.timeout(30.seconds).asserting(_ shouldBe List("first", "second", "third"))
    }

    "let an aggregate past the validator of the collection it writes with $out, when told to" in {
      val program =
        for
          client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
          database <- client.getDatabase("query_it")
          _        <- database.createCollection("validated_out", CreateCollectionOptions.default.withValidator(BsonDocument("age", BsonDocument("$gt", BsonInt32(100)))))
          source   <- seeded("out_source")
          refused  <- source.aggregate[Person](Seq(Stage.out("validated_out"))).all.attempt
          _        <- source.aggregate[Person](Seq(Stage.out("validated_out"))).bypassDocumentValidation(true).all
          written  <- database.getCollection[Person]("validated_out").flatMap(_.count())
        yield (refused.isLeft, written)

      program.asserting(_ shouldBe (true, 4L))
    }
  }
