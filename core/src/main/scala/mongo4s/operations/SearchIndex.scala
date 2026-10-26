package mongo4s.operations

import scala.annotation.unused

import org.bson.{BsonArray, BsonBoolean, BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.FieldNaming
import mongo4s.{ElementOf, Field, FieldPath, NumericOf}

import scala.jdk.CollectionConverters.given

enum VectorSimilarity(val wireName: String):
  case Cosine     extends VectorSimilarity("cosine")
  case Euclidean  extends VectorSimilarity("euclidean")
  case DotProduct extends VectorSimilarity("dotProduct")

enum SearchIndex[E](val name: String):
  case Search[T](override val name: String, definition: BsonDocument) extends SearchIndex[T](name)

  case Vector[T](
      override val name: String,
      vectors: List[(FieldPath, Int, VectorSimilarity)],
      filters: List[FieldPath],
  ) extends SearchIndex[T](name)

  def definition(naming: FieldNaming): BsonDocument =
    this match
      case Search(_, definition)       => definition
      case Vector(_, vectors, filters) =>
        val vectorFields = vectors.map { (path, dimensions, similarity) =>
          BsonDocument("type", BsonString("vector"))
            .append("path", BsonString(path.render(naming)))
            .append("numDimensions", BsonInt32(dimensions))
            .append("similarity", BsonString(similarity.wireName)): BsonValue
        }
        val filterFields = filters.map(path => BsonDocument("type", BsonString("filter")).append("path", BsonString(path.render(naming))): BsonValue)
        BsonDocument("fields", BsonArray((vectorFields ++ filterFields).asJava))

object SearchIndex:

  def dynamic[E](name: String = SearchOptions.DefaultIndex): SearchIndex[E] =
    Search(name, BsonDocument("mappings", BsonDocument("dynamic", BsonBoolean(true))))

  def of[E](name: String, definition: BsonDocument): SearchIndex[E] = Search(name, definition)

  def vector[E, C, A](name: String, field: Field[E, C], dimensions: Int, similarity: VectorSimilarity)(using
      @unused element: ElementOf[C, A],
      @unused numeric: NumericOf[A, ?],
  ): Vector[E] =
    require(dimensions >= 1 && dimensions <= 8192, s"a vector index holds 1 to 8192 dimensions, got $dimensions")
    Vector(name, List((field.path, dimensions, similarity)), Nil)

  extension [E](index: Vector[E]) def filteringOn[A](field: Field[E, A]): Vector[E] = Vector(index.name, index.vectors, index.filters :+ field.path)
