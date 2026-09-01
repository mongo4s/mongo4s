package mongo4s.results

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import com.mongodb.bulk.BulkWriteResult as DriverBulkWriteResult
import com.mongodb.client.result.{DeleteResult as DriverDeleteResult, UpdateResult as DriverUpdateResult}
import com.mongodb.client.result.{InsertManyResult as DriverInsertManyResult, InsertOneResult as DriverInsertOneResult}

final class UnacknowledgedWriteResultSpec extends AnyWordSpec, Matchers:

  "a write the server never acknowledged" should {
    "yield an empty InsertOneResult rather than throwing" in {
      InsertOneResult.fromDriver(DriverInsertOneResult.unacknowledged()).insertedId shouldBe None
    }

    "yield an empty InsertManyResult rather than throwing" in {
      InsertManyResult.fromDriver(DriverInsertManyResult.unacknowledged()).insertedIds shouldBe Nil
    }

    "yield an empty UpdateResult rather than throwing" in {
      UpdateResult.fromDriver(DriverUpdateResult.unacknowledged()) shouldBe UpdateResult.none
    }

    "yield an empty DeleteResult rather than throwing" in {
      DeleteResult.fromDriver(DriverDeleteResult.unacknowledged()) shouldBe DeleteResult.none
    }

    "yield an empty BulkWriteResult rather than throwing" in {
      BulkWriteResult.fromDriver(DriverBulkWriteResult.unacknowledged()) shouldBe BulkWriteResult.none
    }
  }
