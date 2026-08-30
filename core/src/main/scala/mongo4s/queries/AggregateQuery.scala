package mongo4s.queries

import scala.concurrent.duration.FiniteDuration

import org.bson.BsonDocument

import mongo4s.Streamable

trait AggregateQuery[F[*], S[*], A]:
  def allowDiskUse(allow: Boolean): AggregateQuery[F, S, A]

  def hint(keys: BsonDocument): AggregateQuery[F, S, A]
  def collation(collation: com.mongodb.client.model.Collation): AggregateQuery[F, S, A]
  def maxTime(duration: FiniteDuration): AggregateQuery[F, S, A]
  def batchSize(n: Int): AggregateQuery[F, S, A]
  def comment(value: String): AggregateQuery[F, S, A]

  def first: F[Option[A]]
  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]

  def attempting: DecodeAttempts[F, S, A]
