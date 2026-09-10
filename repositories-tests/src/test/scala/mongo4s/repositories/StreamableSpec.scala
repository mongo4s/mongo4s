package mongo4s.repositories

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

final class StreamableSpec extends AnyWordSpec, Matchers:

  "Streamable" should {

    "not be constructible from outside mongo4s, since a backend may carry evidence in it" in {
      typeChecks("mongo4s.Streamable.instance[fs2.Stream[cats.effect.IO, *], Int]") shouldBe false
    }

    "still be supplied by a runtime module for its own stream type" in {
      typeChecks(
        "import mongo4s.cats.CatsInstances.given; summon[mongo4s.Streamable[mongo4s.cats.CatsStream[cats.effect.IO], Int]]"
      ) shouldBe true
    }
  }
