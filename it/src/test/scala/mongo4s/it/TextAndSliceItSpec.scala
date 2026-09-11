package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO
import org.bson.{BsonArray, BsonDocument, BsonString, BsonValue}

import scala.jdk.CollectionConverters.given

import mongo4s.cats.CatsStream
import mongo4s.operations.{Filter, Index, Projection, Sort}
import mongo4s.testkit.FakeMongoCollection
import mongo4s.bson.BsonDocumentCodec
import mongo4s.{Field, MongoClient, MongoCollection}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

final class TextAndSliceItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:

  private val container = new MongoDBContainer("mongo:8.2")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val title    = Field.stored[BsonDocument, String]("title")
  private val comments = Field.stored[BsonDocument, List[String]]("comments")

  private def article(name: String, body: String): BsonDocument =
    BsonDocument("title", BsonString(name)).append("body", BsonString(body))

  private def thread(id: String, howMany: Int): BsonDocument =
    BsonDocument("title", BsonString(id))
      .append("comments", BsonArray(List.tabulate(howMany)(i => BsonString(('a' + i).toChar.toString): BsonValue).asJava))

  private def collection(name: String, seed: List[BsonDocument]): IO[MongoCollection[IO, S, BsonDocument]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("text_slice_it")
      collection <- database.getCollection[BsonDocument](name)
      _          <- collection.insertMany(seed)
    yield collection

  private def titlesOf(documents: List[BsonDocument]): List[String] =
    documents.map(_.getString("title").getValue)

  private def commentsOf(document: BsonDocument): List[String] =
    document.getArray("comments").getValues.asScala.map(_.asString.getValue).toList

  "a $text search sorted by relevance" should {

    "return the best match first, which is what Sort.byTextScore is for" in {
      val program =
        for
          collection <- collection(
                          "ranked",
                          List(
                            article("weak", "mongo appears once here"),
                            article("strong", "mongo mongo mongo everywhere in this mongo text"),
                          ),
                        )
          _          <- collection.createIndex(Index.text(Field.stored[BsonDocument, String]("body")))
          ranked     <- collection.find(Filter.text("mongo")).sort(Sort.byTextScore()).all
        yield titlesOf(ranked)

      program.timeout(30.seconds).asserting(_ shouldBe List("strong", "weak"))
    }
  }

  "$slice" should {

    def seededFake: IO[FakeMongoCollection[IO, S, BsonDocument]] =
      val fake = FakeMongoCollection[IO, S, BsonDocument](summon[BsonDocumentCodec[BsonDocument]], _ => fs2.Stream.empty)
      fake.insertOne(thread("t", 5)).as(fake)

    def parity(name: String, projection: Projection[BsonDocument]): IO[(List[String], List[String])] =
      for
        collection <- collection(name, List(thread("t", 5)))
        fake       <- seededFake
        fromServer <- collection.find(title.equalTo("t")).projection(projection).first
        fromFake   <- fake.find(title.equalTo("t")).projection(projection).first
      yield (fromServer.map(commentsOf).getOrElse(Nil), fromFake.map(commentsOf).getOrElse(Nil))

    "cut an array to its first elements, exactly as the fake does" in {
      parity("first", Projection.empty[BsonDocument].slice(comments, 2))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => (fromFake, fromServer) shouldBe (List("a", "b"), List("a", "b")))
    }

    "cut to the last elements on a negative count, exactly as the fake does" in {
      parity("last", Projection.empty[BsonDocument].slice(comments, -2))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => (fromFake, fromServer) shouldBe (List("d", "e"), List("d", "e")))
    }

    "cut a window from an offset, exactly as the fake does" in {
      parity("window", Projection.empty[BsonDocument].sliceFrom(comments, skip = 1, count = 2))
        .timeout(30.seconds)
        .asserting((fromServer, fromFake) => (fromFake, fromServer) shouldBe (List("b", "c"), List("b", "c")))
    }
  }
