package mongo4s.operations

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import com.mongodb.ReadConcern

final class SessionOptionsSpec extends AnyWordSpec, Matchers:

  "SessionOptions" should {
    "leave causal consistency at the driver's default, which is on, unless asked" in {
      val driver = SessionOptions.default.toDriver

      driver.isCausallyConsistent shouldBe true
      driver.isSnapshot shouldBe false
    }

    "carry a snapshot, causal consistency and the transaction options sessions start with" in {
      val transaction = TransactionOptions.default.withReadConcern(ReadConcern.MAJORITY)

      SessionOptions.snapshot.toDriver.isSnapshot shouldBe true
      SessionOptions.default.withCausalConsistency(false).toDriver.isCausallyConsistent shouldBe false
      SessionOptions.default.withDefaultTransactionOptions(transaction).toDriver.getDefaultTransactionOptions.getReadConcern shouldBe ReadConcern.MAJORITY
    }

    "refuse a session that is both a snapshot and causally consistent, whichever is asked for first" in {
      the[IllegalArgumentException] thrownBy SessionOptions.snapshot.withCausalConsistency(true)
      the[IllegalArgumentException] thrownBy SessionOptions.default.withCausalConsistency(true).withSnapshot
    }
  }
