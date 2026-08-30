package mongo4s.results

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonString, BsonValue}

final class BulkWriteResultSpec extends AnyWordSpec, Matchers:

  private def id(value: String): BsonValue = BsonString(value)

  private def upserts(entries: (Int, String)*): BulkWriteResult =
    BulkWriteResult.none.copy(upsertedIds = entries.map((index, value) => index -> id(value)).toMap)

  "combine" should {
    "sum every count" in {
      val left  = BulkWriteResult(insertedCount = 2, matchedCount = 3, modifiedCount = 1, deletedCount = 4, upsertedIds = Map.empty)
      val right = BulkWriteResult(insertedCount = 5, matchedCount = 0, modifiedCount = 6, deletedCount = 7, upsertedIds = Map.empty)

      BulkWriteResult.combine(Seq(left, right)) shouldBe
        BulkWriteResult(insertedCount = 7, matchedCount = 3, modifiedCount = 7, deletedCount = 11, upsertedIds = Map.empty)
    }

    "produce the identity for no results" in {
      BulkWriteResult.combine(Nil) shouldBe BulkWriteResult.none
    }
  }

  "shiftUpsertedIds" should {
    "rebase every index onto the batch's position in the whole command sequence" in {
      upserts(0 -> "a", 3 -> "b").shiftUpsertedIds(500).upsertedIds shouldBe Map(500 -> id("a"), 503 -> id("b"))
    }

    "leave a first batch alone" in {
      val first = upserts(0 -> "a")
      first.shiftUpsertedIds(0) shouldBe first
    }
  }

  "a batched bulk write" should {
    "keep one entry per upsert once each batch is rebased" in {
      val batchSize = 2
      val batches   = List(upserts(0 -> "a", 1 -> "b"), upserts(0 -> "c", 1 -> "d"), upserts(0 -> "e"))
      val offsets   = batches.indices.map(_ * batchSize)

      val merged = BulkWriteResult.combine(batches.zip(offsets).map((result, offset) => result.shiftUpsertedIds(offset)))

      merged.upsertedIds shouldBe Map(0 -> id("a"), 1 -> id("b"), 2 -> id("c"), 3 -> id("d"), 4 -> id("e"))
    }

    "collapse to a single entry per index if the batches are not rebased" in {
      val merged = BulkWriteResult.combine(List(upserts(0 -> "a", 1 -> "b"), upserts(0 -> "c", 1 -> "d")))

      merged.upsertedIds should have size 2
    }
  }
