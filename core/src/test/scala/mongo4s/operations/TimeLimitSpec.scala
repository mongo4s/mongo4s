package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.internal.QueryOptions
import mongo4s.changestream.WatchOptions

import scala.concurrent.duration.given

final class TimeLimitSpec extends AnyWordSpec, Matchers:

  "a query's maxTime" should {
    "keep a whole number of milliseconds" in {
      QueryOptions.empty.withMaxTime(250.millis).maxTimeMillis shouldBe Some(250L)
    }

    "refuse a sub-millisecond limit, which would truncate to 0 and so lift the limit entirely" in {
      val thrown = the[IllegalArgumentException] thrownBy QueryOptions.empty.withMaxTime(500.micros)
      thrown.getMessage should include("at least one millisecond")
    }
  }

  "a count's maxTime" should {
    "refuse a sub-millisecond limit" in {
      val thrown = the[IllegalArgumentException] thrownBy CountOptions.default.withMaxTime(500.micros)
      thrown.getMessage should include("at least one millisecond")
    }
  }

  "a transaction's maxCommitTime" should {
    "refuse a sub-millisecond limit when it is set, not when the transaction starts" in {
      val thrown = the[IllegalArgumentException] thrownBy TransactionOptions.default.withMaxCommitTime(500.micros)
      thrown.getMessage should include("at least one millisecond")
    }
  }

  "a change stream's maxAwaitTime" should {
    "refuse a sub-millisecond wait" in {
      val thrown = the[IllegalArgumentException] thrownBy WatchOptions.default[Any].withMaxAwaitTime(500.micros)
      thrown.getMessage should include("at least one millisecond")
    }
  }
