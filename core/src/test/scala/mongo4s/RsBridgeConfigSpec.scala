package mongo4s

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.given

final class RsBridgeConfigSpec extends AnyWordSpec, Matchers:

  "RsBridgeConfig" should {

    "carry the driver's own defaults" in {
      RsBridgeConfig.default.bufferSize shouldBe 256
      RsBridgeConfig.default.timeout shouldBe None
      RsBridgeConfig.default.strictSingleResult shouldBe false
    }

    "keep the fields a builder did not touch" in {
      val configured = RsBridgeConfig.default.withBufferSize(512).withTimeout(5.seconds)

      configured.bufferSize shouldBe 512
      configured.timeout shouldBe Some(5.seconds)
      configured.strictSingleResult shouldBe false
    }

    "drop a timeout that was set" in {
      RsBridgeConfig.default.withTimeout(5.seconds).withoutTimeout.timeout shouldBe None
    }

    "refuse a buffer size that would stall the first stream using it" in {
      intercept[IllegalArgumentException](RsBridgeConfig.default.withBufferSize(0))
      intercept[IllegalArgumentException](RsBridgeConfig.default.withBufferSize(-1))
    }

    "supply the default through the companion's given" in {
      summon[RsBridgeConfig].bufferSize shouldBe RsBridgeConfig.default.bufferSize
    }
  }
