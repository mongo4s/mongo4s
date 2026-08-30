package mongo4s.results

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

final class DeleteResultSpec extends AnyWordSpec, Matchers:

  "DeleteResult" should {
    "carry the count it was built from" in {
      DeleteResult(3).deletedCount shouldBe 3L
      DeleteResult.none.deletedCount shouldBe 0L
    }

    "report whether anything was removed" in {
      DeleteResult(1).deletedAny shouldBe true
      DeleteResult(0).deletedAny shouldBe false
      DeleteResult.none.deletedAny shouldBe false
    }

    "compare by count" in {
      DeleteResult(3) shouldBe DeleteResult(3)
      DeleteResult(3) should not be DeleteResult(4)
      DeleteResult(0) shouldBe DeleteResult.none
    }

    "sum across results" in {
      val merged = DeleteResult(List(DeleteResult(2), DeleteResult(3), DeleteResult.none).map(_.deletedCount).sum)

      merged.deletedCount shouldBe 5L
    }

    "not be interchangeable with a plain Long" in {
      "val n: Long = DeleteResult(1)" shouldNot typeCheck
      "val r: DeleteResult = 1L" shouldNot typeCheck
    }
  }
