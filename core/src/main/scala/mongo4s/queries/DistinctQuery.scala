package mongo4s.queries

import mongo4s.Streamable

trait DistinctQuery[F[*], S[*], A]:
  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]
