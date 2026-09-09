package mongo4s.queries

import scala.quoted.*
import scala.NamedTuple.AnyNamedTuple
import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument
import com.mongodb.ExplainVerbosity

import mongo4s.{SelectMacro, Streamable}
import mongo4s.operations.{Filter, Projection, Sort}
import mongo4s.bson.{BsonDocumentDecoder, FieldNaming}

trait FindQuery[F[*], S[*], A]:
  def filter(filter: Filter[A]): FindQuery[F, S, A]
  def sort(sort: Sort[A]): FindQuery[F, S, A]
  def projection(projection: Projection[A]): FindQuery[F, S, A]
  def skip(n: Int): FindQuery[F, S, A]
  def limit(n: Int): FindQuery[F, S, A]
  def hint(keys: BsonDocument): FindQuery[F, S, A]
  def collation(collation: com.mongodb.client.model.Collation): FindQuery[F, S, A]
  def maxTime(duration: FiniteDuration): FindQuery[F, S, A]
  def batchSize(n: Int): FindQuery[F, S, A]
  def comment(value: String): FindQuery[F, S, A]

  def explain(verbosity: ExplainVerbosity = ExplainVerbosity.QUERY_PLANNER): F[BsonDocument]

  def first: F[Option[A]]

  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]

  def attempting: DecodeAttempts[F, S, A]

  def selecting[B](projection: Projection[A], decoder: FieldNaming => BsonDocumentDecoder[B]): SelectQuery[F, S, B]

  inline def selectAs[K <: AnyNamedTuple]: SelectQuery[F, S, K] =
    ${ SelectMacro.impl[F, S, A, K]('this) }
