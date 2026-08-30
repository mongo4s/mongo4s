package mongo4s.queries

import scala.concurrent.duration.FiniteDuration

import mongo4s.Streamable

trait DistinctQuery[F[*], S[*], A]:
  def collation(collation: com.mongodb.client.model.Collation): DistinctQuery[F, S, A]
  def maxTime(duration: FiniteDuration): DistinctQuery[F, S, A]
  def batchSize(n: Int): DistinctQuery[F, S, A]

  def first: F[Option[A]]
  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]

  def attempting: DecodeAttempts[F, S, A]
