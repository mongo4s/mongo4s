package mongo4s.queries

import scala.NamedTuple.AnyNamedTuple
import scala.concurrent.duration.FiniteDuration
import scala.quoted.*

import org.bson.BsonDocument

import mongo4s.bson.{BsonDocumentDecoder, FieldNaming}
import mongo4s.operations.{Filter, Projection, Sort}
import mongo4s.{SelectMacro, Streamable}

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

  def first: F[Option[A]]

  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]

  def attempting: DecodeAttempts[F, S, A]

  def selecting[B](projection: Projection[A], decoder: FieldNaming => BsonDocumentDecoder[B]): SelectQuery[F, S, B]

  inline def selectAs[K <: AnyNamedTuple]: SelectQuery[F, S, K] =
    ${ SelectMacro.impl[F, S, A, K]('this) }
