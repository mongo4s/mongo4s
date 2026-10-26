package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer

import cats.effect.IO
import org.bson.BsonDocument

import mongo4s.cats.CatsStream
import mongo4s.bson.direct.WireCodec
import mongo4s.{Field, MongoClient, MongoCollection}
import mongo4s.operations.{SearchCount, SearchIndex, SearchOperator, SearchOptions, Stage, VectorSimilarity}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object SearchItSpec:
  final case class Movie(id: String, title: String, plot: String, year: Int, embedding: List[Double]) derives WireCodec

final class SearchItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import SearchItSpec.Movie

  private val container = new MongoDBAtlasLocalContainer("mongodb/mongodb-atlas-local:8.2.11")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val title     = Field.of[Movie, String](_.title)
  private val plot      = Field.of[Movie, String](_.plot)
  private val year      = Field.of[Movie, Int](_.year)
  private val embedding = Field.of[Movie, List[Double]](_.embedding)

  private val movies = List(
    Movie("1", "The Matrix", "a hacker learns reality is simulated", 1999, List(0.1, 0.9, 0.2)),
    Movie("2", "Matrix Reloaded", "the hacker fights the agents", 2003, List(0.2, 0.8, 0.1)),
    Movie("3", "Cars", "a race car learns humility", 2006, List(0.9, 0.1, 0.5)),
  )

  private val vectors = SearchIndex.vector("vectors", embedding, 3, VectorSimilarity.Cosine).filteringOn(year)

  private def indexed(name: String): IO[MongoCollection[IO, S, Movie]] =
    for
      client     <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database   <- client.getDatabase("search_it")
      collection <- database.getDirectCollection[Movie](name)
      _          <- collection.insertMany(movies)
      _          <- collection.createSearchIndexes(List(SearchIndex.dynamic[Movie](), vectors))
      _          <- queryable(collection, Set("default", "vectors"))
    yield collection

  private def queryable(collection: MongoCollection[IO, S, Movie], names: Set[String]): IO[Unit] =
    def ready(indexes: List[BsonDocument]): Boolean =
      names.forall(name => indexes.exists(index => index.getString("name").getValue == name && index.getBoolean("queryable").getValue))

    def poll(remaining: Int): IO[Unit] =
      collection.listSearchIndexes.flatMap { indexes =>
        if ready(indexes) then IO.unit
        else if remaining == 0 then IO.raiseError(IllegalStateException(s"search indexes $names never became queryable: $indexes"))
        else IO.sleep(1.second) >> poll(remaining - 1)
      }

    poll(180)
  end queryable

  "search indexes" should {
    "be created, listed with their types, redefined and dropped" in {
      val program =
        for
          collection <- indexed("indexes")
          listed     <- collection.listSearchIndexes
          _          <- collection.updateSearchIndex(
                          SearchIndex.of[Movie]("default", BsonDocument.parse("""{"mappings": {"dynamic": false, "fields": {"title": {"type": "string"}}}}"""))
                        )
          _          <- collection.dropSearchIndex("vectors")
          remaining  <- collection.listSearchIndexes
        yield (listed.map(index => index.getString("name").getValue -> index.getString("type").getValue).toSet, remaining.map(_.getString("name").getValue))

      program.timeout(4.minutes).asserting(_ shouldBe (Set("default" -> "search", "vectors" -> "vectorSearch"), List("default")))
    }
  }

  "$search" should {
    "find the documents a text query matches, through the dynamic mapping" in {
      val program =
        for
          collection <- indexed("text")
          found      <- collection.aggregateDirect[Movie](Seq(Stage.search(SearchOperator.text(plot, "hacker")))).all
        yield found.map(_.title).toSet

      program.timeout(4.minutes).asserting(_ shouldBe Set("The Matrix", "Matrix Reloaded"))
    }

    "combine a required phrase with a range it must also satisfy" in {
      val operator = SearchOperator.compound[Movie](
        must = List(SearchOperator.text(title, "matrix")),
        filter = List(SearchOperator.atLeast(year, 2000)),
      )

      val program =
        for
          collection <- indexed("compound")
          found      <- collection.aggregateDirect[Movie](Seq(Stage.search(operator))).all
        yield found.map(_.title)

      program.timeout(4.minutes).asserting(_ shouldBe List("Matrix Reloaded"))
    }
  }

  "$searchMeta" should {
    "count the matches without returning them" in {
      val program =
        for
          collection <- indexed("meta")
          meta       <- collection.aggregate[BsonDocument](Seq(Stage.searchMeta(SearchOperator.text(plot, "learns"), SearchOptions.default.withCount(SearchCount.total)))).first
        yield meta.map(_.getDocument("count").getNumber("total").longValue)

      program.timeout(4.minutes).asserting(_ shouldBe Some(2L))
    }
  }

  "$vectorSearch" should {
    "return the nearest documents, narrowed by a filter on an indexed filter field" in {
      val program =
        for
          collection <- indexed("vectors")
          nearest    <- collection.aggregateDirect[Movie](Seq(Stage.vectorSearch("vectors", embedding, List(0.1, 0.9, 0.2), limit = 2, numCandidates = 10))).all
          recent     <-
            collection
              .aggregateDirect[Movie](Seq(Stage.vectorSearch("vectors", embedding, List(0.1, 0.9, 0.2), limit = 2, numCandidates = 10, filter = Some(year.gte(2000)))))
              .all
          exact      <- collection.aggregateDirect[Movie](Seq(Stage.exactVectorSearch("vectors", embedding, List(0.9, 0.1, 0.5), limit = 1))).all
        yield (nearest.map(_.title), recent.map(_.title), exact.map(_.title))

      program.timeout(4.minutes).asserting(_ shouldBe (List("The Matrix", "Matrix Reloaded"), List("Matrix Reloaded", "Cars"), List("Cars")))
    }
  }
