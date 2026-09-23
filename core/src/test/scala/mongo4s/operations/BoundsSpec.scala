package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.internal.QueryOptions
import mongo4s.changestream.WatchOptions

final class BoundsSpec extends AnyWordSpec, Matchers:

  "a limit" should {
    "refuse zero, which MongoDB reads as no limit at all, and anything below it" in {
      (the[IllegalArgumentException] thrownBy CountOptions.default.withLimit(0)).getMessage should include("no limit at all")
      an[IllegalArgumentException] should be thrownBy CountOptions.default.withLimit(-5)
      an[IllegalArgumentException] should be thrownBy Stage.limit[Any](0)
    }

    "accept one and above" in {
      CountOptions.default.withLimit(1).limit shouldBe Some(1)
      Stage.limit[Any](10) shouldBe Stage.Limit(10)
    }
  }

  "a skip" should {
    "refuse a negative count, which the server refuses, and accept zero" in {
      an[IllegalArgumentException] should be thrownBy CountOptions.default.withSkip(-1)
      an[IllegalArgumentException] should be thrownBy Stage.skip[Any](-1)
      CountOptions.default.withSkip(0).skip shouldBe Some(0)
    }
  }

  "a batch size" should {
    "refuse zero and below, which MongoDB reads as its default or as a single batch" in {
      an[IllegalArgumentException] should be thrownBy QueryOptions.empty.withBatchSize(0)
      an[IllegalArgumentException] should be thrownBy WatchOptions.default[Any].withBatchSize(-1)
      QueryOptions.empty.withBatchSize(100).batchSize shouldBe Some(100)
    }
  }

  "a sample" should {
    "refuse a size of zero" in {
      an[IllegalArgumentException] should be thrownBy Stage.sample[Any](0)
    }
  }
