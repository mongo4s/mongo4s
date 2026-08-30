package mongo4s.queries

import mongo4s.Streamable
import mongo4s.bson.DecodeResult

trait DecodeAttempts[F[*], S[*], A]:
  def all: F[List[DecodeResult[A]]]
  def stream(using Streamable[S, DecodeResult[A]]): S[DecodeResult[A]]
