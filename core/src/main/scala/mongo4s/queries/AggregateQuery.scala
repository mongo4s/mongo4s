package mongo4s.queries

import mongo4s.Streamable

trait AggregateQuery[F[*], S[*], A]:
  def allowDiskUse(allow: Boolean): AggregateQuery[F, S, A]

  def first: F[Option[A]]
  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]
