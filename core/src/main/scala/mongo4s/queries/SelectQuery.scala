package mongo4s.queries

import mongo4s.Streamable

trait SelectQuery[F[*], S[*], A]:
  def first: F[Option[A]]
  def all: F[List[A]]
  def stream(using Streamable[S, A]): S[A]
  def attempting: DecodeAttempts[F, S, A]
