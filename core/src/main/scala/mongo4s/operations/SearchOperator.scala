package mongo4s.operations

import org.bson.{BsonArray, BsonBoolean, BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.{BsonEncoder, FieldNaming}
import mongo4s.{ComparableOf, Field, FieldPath, TextOf}

import scala.jdk.CollectionConverters.given

enum SearchOperator[E](val key: String):
  case Text[T](query: String, paths: List[FieldPath], maxEdits: Option[Int]) extends SearchOperator[T]("text")
  case Phrase[T](query: String, paths: List[FieldPath], slop: Option[Int])   extends SearchOperator[T]("phrase")
  case Autocomplete[T](query: String, path: FieldPath)                       extends SearchOperator[T]("autocomplete")
  case Wildcard[T](query: String, paths: List[FieldPath])                    extends SearchOperator[T]("wildcard")
  case Regex[T](query: String, paths: List[FieldPath])                       extends SearchOperator[T]("regex")
  case Equals[T](path: FieldPath, value: BsonValue)                          extends SearchOperator[T]("equals")
  case Exists[T](path: FieldPath)                                            extends SearchOperator[T]("exists")
  case Raw[T](document: BsonDocument)                                        extends SearchOperator[T]("")

  case Range[T](
      path: FieldPath,
      lower: Option[(String, BsonValue)],
      upper: Option[(String, BsonValue)],
  ) extends SearchOperator[T]("range")

  case Compound[T](
      must: List[SearchOperator[T]],
      should: List[SearchOperator[T]],
      mustNot: List[SearchOperator[T]],
      filter: List[SearchOperator[T]],
      minimumShouldMatch: Option[Int],
  ) extends SearchOperator[T]("compound")

  def toBson(naming: FieldNaming): BsonDocument =
    this match
      case Raw(document) => document
      case _             => BsonDocument(key, body(naming))

  private def body(naming: FieldNaming): BsonDocument =
    this match
      case Text(query, paths, maxEdits) =>
        val text = BsonDocument("query", BsonString(query)).append("path", SearchOperator.paths(paths, naming))
        maxEdits.foreach(edits => text.append("fuzzy", BsonDocument("maxEdits", BsonInt32(edits))))
        text

      case Phrase(query, paths, slop) =>
        val phrase = BsonDocument("query", BsonString(query)).append("path", SearchOperator.paths(paths, naming))
        slop.foreach(value => phrase.append("slop", BsonInt32(value)))
        phrase

      case Autocomplete(query, path) => BsonDocument("query", BsonString(query)).append("path", BsonString(path.render(naming)))
      case Wildcard(query, paths)    => SearchOperator.analyzed(query, paths, naming)
      case Regex(query, paths)       => SearchOperator.analyzed(query, paths, naming)
      case Equals(path, value)       => BsonDocument("path", BsonString(path.render(naming))).append("value", value)
      case Exists(path)              => BsonDocument("path", BsonString(path.render(naming)))

      case Range(path, lower, upper) =>
        val range = BsonDocument("path", BsonString(path.render(naming)))
        lower.foreach((bound, value) => range.append(bound, value))
        upper.foreach((bound, value) => range.append(bound, value))
        range

      case Compound(must, should, mustNot, filter, minimumShouldMatch) =>
        val compound = BsonDocument()
        List("must" -> must, "should" -> should, "mustNot" -> mustNot, "filter" -> filter).foreach { (clause, operators) =>
          if operators.nonEmpty
          then compound.append(clause, BsonArray(operators.map(_.toBson(naming): BsonValue).asJava)): Unit
        }
        minimumShouldMatch.foreach(value => compound.append("minimumShouldMatch", BsonInt32(value)))
        compound

      case Raw(document) => document
  end body

object SearchOperator:

  private def paths(paths: List[FieldPath], naming: FieldNaming): BsonValue =
    paths match
      case single :: Nil => BsonString(single.render(naming))
      case many          => BsonArray(many.map(path => BsonString(path.render(naming)): BsonValue).asJava)

  private def analyzed(query: String, paths: List[FieldPath], naming: FieldNaming): BsonDocument =
    BsonDocument("query", BsonString(query))
      .append("path", this.paths(paths, naming))
      .append("allowAnalyzedField", BsonBoolean(true))

  def text[E, A](field: Field[E, A], query: String)(using TextOf[A]): Text[E]                   = Text(query, List(field.path), None)
  def phrase[E, A](field: Field[E, A], query: String)(using TextOf[A]): Phrase[E]               = Phrase(query, List(field.path), None)
  def autocomplete[E, A](field: Field[E, A], query: String)(using TextOf[A]): SearchOperator[E] = Autocomplete(query, field.path)
  def wildcard[E, A](field: Field[E, A], query: String)(using TextOf[A]): SearchOperator[E]     = Wildcard(query, List(field.path))
  def regex[E, A](field: Field[E, A], query: String)(using TextOf[A]): SearchOperator[E]        = Regex(query, List(field.path))
  def exists[E, A](field: Field[E, A]): SearchOperator[E]                                       = Exists(field.path)
  def raw[E](document: BsonDocument): SearchOperator[E]                                         = Raw(document)

  def equalTo[E, A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): SearchOperator[E] =
    Equals(field.path, encoder.encode(value))

  def between[E, C, A](field: Field[E, C], from: A, until: A)(using comparable: ComparableOf[C, A]): SearchOperator[E] =
    Range(field.path, Some("gte" -> comparable.encode(from)), Some("lt" -> comparable.encode(until)))

  def atLeast[E, C, A](field: Field[E, C], from: A)(using comparable: ComparableOf[C, A]): SearchOperator[E] =
    Range(field.path, Some("gte" -> comparable.encode(from)), None)

  def below[E, C, A](field: Field[E, C], until: A)(using comparable: ComparableOf[C, A]): SearchOperator[E] =
    Range(field.path, None, Some("lt" -> comparable.encode(until)))

  def compound[E](
      must: Seq[SearchOperator[E]] = Nil,
      should: Seq[SearchOperator[E]] = Nil,
      mustNot: Seq[SearchOperator[E]] = Nil,
      filter: Seq[SearchOperator[E]] = Nil,
      minimumShouldMatch: Option[Int] = None,
  ): SearchOperator[E] =
    require(must.nonEmpty || should.nonEmpty || mustNot.nonEmpty || filter.nonEmpty, "a compound search needs at least one clause")
    minimumShouldMatch.foreach(value =>
      require(value >= 0 && value <= should.size, s"minimumShouldMatch $value is outside 0 to ${should.size}, the should clauses there are")
    )
    Compound(must.toList, should.toList, mustNot.toList, filter.toList, minimumShouldMatch)

  extension [E](operator: Text[E])
    def orIn[A](field: Field[E, A])(using TextOf[A]): Text[E] = Text(operator.query, operator.paths :+ field.path, operator.maxEdits)

    def fuzzy(maxEdits: Int): Text[E] =
      require(maxEdits == 1 || maxEdits == 2, s"a fuzzy text search allows 1 or 2 edits, got $maxEdits")
      Text(operator.query, operator.paths, Some(maxEdits))

  extension [E](operator: Phrase[E])
    def orIn[A](field: Field[E, A])(using TextOf[A]): Phrase[E] = Phrase(operator.query, operator.paths :+ field.path, operator.slop)

    def withSlop(slop: Int): Phrase[E] =
      require(slop >= 0, s"a phrase slop cannot be negative, got $slop")
      Phrase(operator.query, operator.paths, Some(slop))
end SearchOperator
