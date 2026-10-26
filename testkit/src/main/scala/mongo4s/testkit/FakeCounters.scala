package mongo4s.testkit

import mongo4s.{Effect, RsBridge}
import mongo4s.repositories.{Counter, MongoCounters}

final class FakeCounters[F[*], S[*]](val fake: FakeMongoCollection[F, S, Counter])(using Effect[F]) extends MongoCounters[F, S](fake)

object FakeCounters:

  def apply[F[*], S[*]]()(using F: Effect[F], rs: RsBridge[F, S]): FakeCounters[F, S] =
    new FakeCounters(FakeMongoCollection[F, S, Counter](Counter.codec, MongoCounters.DefaultCollection))
