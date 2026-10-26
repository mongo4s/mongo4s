package mongo4s.operations

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.Field
import mongo4s.bson.FieldNaming

import mongo4s.bson.BsonInstances.given

object SearchSpec:
  final case class Movie(title: String, fullPlot: String, releaseYear: Int, plotEmbedding: List[Double], genre: String)

final class SearchSpec extends AnyWordSpec, Matchers:
  import SearchSpec.Movie

  private val title         = Field.of[Movie, String](_.title)
  private val fullPlot      = Field.of[Movie, String](_.fullPlot)
  private val releaseYear   = Field.of[Movie, Int](_.releaseYear)
  private val plotEmbedding = Field.of[Movie, List[Double]](_.plotEmbedding)
  private val genre         = Field.of[Movie, String](_.genre)

  private def snake(stage: Stage[Movie]): String = stage.toBson(FieldNaming.snakeCase).toJson

  "$search" should {
    "render a text search over several paths through the naming, with fuzzy matching" in {
      snake(Stage.search(SearchOperator.text(title, "matrix").orIn(fullPlot).fuzzy(1))) shouldBe
        """{"$search": {"index": "default", "text": {"query": "matrix", "path": ["title", "full_plot"], "fuzzy": {"maxEdits": 1}}}}"""
    }

    "render a compound of the typed operators, an index name, a count and stored source" in {
      val operator = SearchOperator.compound[Movie](
        must = List(SearchOperator.phrase(fullPlot, "red pill").withSlop(2)),
        should = List(SearchOperator.equalTo(genre, "sci-fi"), SearchOperator.between(releaseYear, 1990, 2000)),
        mustNot = List(SearchOperator.wildcard(title, "*Reloaded*")),
        filter = List(SearchOperator.exists(releaseYear)),
        minimumShouldMatch = Some(1),
      )

      snake(Stage.search(operator, SearchOptions.default.withIndex("movies").withCount(SearchCount.total).withReturnStoredSource)) shouldBe
        """{"$search": {"index": "movies", "compound": {""" +
        """"must": [{"phrase": {"query": "red pill", "path": "full_plot", "slop": 2}}], """ +
        """"should": [{"equals": {"path": "genre", "value": "sci-fi"}}, {"range": {"path": "release_year", "gte": 1990, "lt": 2000}}], """ +
        """"mustNot": [{"wildcard": {"query": "*Reloaded*", "path": "title", "allowAnalyzedField": true}}], """ +
        """"filter": [{"exists": {"path": "release_year"}}], "minimumShouldMatch": 1}, """ +
        """"count": {"type": "total"}, "returnStoredSource": true}}"""
    }

    "render $searchMeta with a lower-bound count" in {
      snake(Stage.searchMeta(SearchOperator.autocomplete(title, "mat"), SearchOptions.default.withCount(SearchCount.lowerBound(500)))) shouldBe
        """{"$searchMeta": {"index": "default", "autocomplete": {"query": "mat", "path": "title"}, "count": {"type": "lowerBound", "threshold": 500}}}"""
    }

    "accept only text fields where the operator analyses text" in {
      typeChecks("SearchOperator.text(releaseYear, \"1999\")") shouldBe false
      typeChecks("SearchOperator.between(title, 1, 2)") shouldBe false
    }

    "refuse the arguments the server would refuse" in {
      an[IllegalArgumentException] should be thrownBy SearchOperator.text(title, "x").fuzzy(3)
      an[IllegalArgumentException] should be thrownBy SearchOperator.compound[Movie]()
      an[IllegalArgumentException] should be thrownBy SearchOperator.compound[Movie](should = List(SearchOperator.exists(title)), minimumShouldMatch = Some(2))
    }
  }

  "$vectorSearch" should {
    "render an approximate search with its candidates and a pre-filter through the naming" in {
      snake(Stage.vectorSearch("plots", plotEmbedding, List(0.1, 0.9), limit = 5, numCandidates = 50, filter = Some(releaseYear.gte(1990)))) shouldBe
        """{"$vectorSearch": {"index": "plots", "path": "plot_embedding", "queryVector": [0.1, 0.9], "limit": 5, "numCandidates": 50, "filter": {"release_year": {"$gte": 1990}}}}"""
    }

    "render an exact search, which takes no candidates" in {
      snake(Stage.exactVectorSearch("plots", plotEmbedding, List(0.1), limit = 3)) shouldBe
        """{"$vectorSearch": {"index": "plots", "path": "plot_embedding", "queryVector": [0.1], "limit": 3, "exact": true}}"""
    }

    "compile only on a numeric array, and refuse fewer candidates than results" in {
      typeChecks("Stage.vectorSearch(\"plots\", title, List(0.1), limit = 5, numCandidates = 50)") shouldBe false
      an[IllegalArgumentException] should be thrownBy Stage.vectorSearch("plots", plotEmbedding, List(0.1), limit = 5, numCandidates = 4)
      an[IllegalArgumentException] should be thrownBy Stage.vectorSearch("plots", plotEmbedding, Nil, limit = 5, numCandidates = 50)
    }
  }

  "a search index" should {
    "define a dynamic mapping by default" in {
      SearchIndex.dynamic[Movie]().definition(FieldNaming.snakeCase).toJson shouldBe """{"mappings": {"dynamic": true}}"""
    }

    "define vector fields and the fields a vector search can filter on, through the naming" in {
      val index = SearchIndex.vector("plots", plotEmbedding, 1536, VectorSimilarity.Cosine).filteringOn(releaseYear)

      index.definition(FieldNaming.snakeCase).toJson shouldBe
        """{"fields": [{"type": "vector", "path": "plot_embedding", "numDimensions": 1536, "similarity": "cosine"}, {"type": "filter", "path": "release_year"}]}"""
    }

    "refuse a dimension count the server would refuse" in {
      an[IllegalArgumentException] should be thrownBy SearchIndex.vector("plots", plotEmbedding, 0, VectorSimilarity.DotProduct)
    }
  }
