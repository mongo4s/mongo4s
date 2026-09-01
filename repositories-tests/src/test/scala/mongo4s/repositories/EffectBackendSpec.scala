package mongo4s.repositories

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import mongo4s.{Effect, ExitCase}

trait EffectBackendSpec[F[*]] extends AnyWordSpec, Matchers:

  protected def effectInstance: Effect[F]

  protected def run[A](fa: F[A]): A

  private given Effect[F]  = effectInstance
  private def F: Effect[F] = effectInstance

  private def runAttempt[A](fa: F[A]): Either[Throwable, A] = run(F.attempt(fa))

  private final class Boom(val tag: String) extends RuntimeException(tag)

  "delay" should {
    "not run its thunk until the effect is run" in {
      val evaluated = AtomicInteger(0)
      val effect    = F.delay(evaluated.incrementAndGet())

      evaluated.get shouldBe 0

      run(effect)
      evaluated.get shouldBe 1
    }

    "re-run its thunk on every run" in {
      val evaluated = AtomicInteger(0)
      val effect    = F.delay(evaluated.incrementAndGet())

      run(effect)
      run(effect)
      evaluated.get shouldBe 2
    }

    "capture a thrown exception in the error channel" in {
      val boom = Boom("delay")
      runAttempt(F.delay[Int](throw boom)) shouldBe Left(boom)
    }
  }

  "suspend" should {
    "not build the inner effect until the effect is run" in {
      val built  = AtomicInteger(0)
      val effect = F.suspend {
        built.incrementAndGet()
        F.pure(1)
      }

      built.get shouldBe 0

      run(effect) shouldBe 1
      built.get shouldBe 1
    }
  }

  "handleErrorWith" should {
    "recover from a raised error" in {
      run(F.handleErrorWith(F.raiseError[Int](Boom("raised")))(_ => F.pure(7))) shouldBe 7
    }

    "recover from an exception thrown inside map" in {
      val thrown: F[Int] = F.map(F.pure(()))(_ => throw Boom("mapped"))
      run(F.handleErrorWith(thrown)(_ => F.pure(7))) shouldBe 7
    }
  }

  "attempt" should {
    "surface a success as Right and an error as Left" in {
      val boom = Boom("attempted")
      run(F.attempt(F.pure(1))) shouldBe Right(1)
      run(F.attempt(F.raiseError[Int](boom))) shouldBe Left(boom)
    }
  }

  "guaranteeCase" should {
    "report Succeeded and keep the value" in {
      val observed = AtomicReference[Option[ExitCase]](None)

      run(F.guaranteeCase(F.pure(1))(exitCase => F.delay(observed.set(Some(exitCase))))) shouldBe 1
      observed.get shouldBe Some(ExitCase.Succeeded)
    }

    "report Errored with the original error and re-raise it" in {
      val boom     = Boom("guaranteed")
      val observed = AtomicReference[Option[ExitCase]](None)

      val result = runAttempt(F.guaranteeCase(F.raiseError[Int](boom))(exitCase => F.delay(observed.set(Some(exitCase)))))

      result shouldBe Left(boom)
      observed.get shouldBe Some(ExitCase.Errored(boom))
    }

    "run the finalizer exactly once" in {
      val runs = AtomicInteger(0)

      run(F.guaranteeCase(F.pure(1))(_ => F.delay(runs.incrementAndGet())))
      runs.get shouldBe 1
    }

    "let the body's error win when the finalizer also fails" in {
      val bodyError      = Boom("body")
      val finalizerError = Boom("finalizer")

      val result = runAttempt(F.guaranteeCase(F.raiseError[Int](bodyError))(_ => F.raiseError[Unit](finalizerError)))

      result shouldBe Left(bodyError)
      bodyError.getSuppressed.toList should contain(finalizerError)
    }

    "raise the finalizer's error when the body succeeded" in {
      val finalizerError = Boom("finalizer-only")

      runAttempt(F.guaranteeCase(F.pure(1))(_ => F.raiseError[Unit](finalizerError))) shouldBe Left(finalizerError)
    }
  }

  "onError" should {
    "run only on failure and preserve the error" in {
      val runs = AtomicInteger(0)
      val boom = Boom("on-error")

      run(F.onError(F.pure(1))(_ => F.delay(runs.incrementAndGet()))) shouldBe 1
      runs.get shouldBe 0

      runAttempt(F.onError(F.raiseError[Int](boom))(_ => F.delay(runs.incrementAndGet()))) shouldBe Left(boom)
      runs.get shouldBe 1
    }
  }

  "bracket" should {
    "release after a successful use" in {
      val released = AtomicInteger(0)

      run(F.bracket(F.pure("resource"))(_ => F.pure(1))(_ => F.delay(released.incrementAndGet()))) shouldBe 1
      released.get shouldBe 1
    }

    "release after a failing use and re-raise the error" in {
      val released = AtomicInteger(0)
      val boom     = Boom("bracket")

      runAttempt(F.bracket(F.pure("resource"))(_ => F.raiseError[Int](boom))(_ => F.delay(released.incrementAndGet()))) shouldBe Left(boom)
      released.get shouldBe 1
    }

    "not release when acquisition fails" in {
      val released = AtomicInteger(0)
      val boom     = Boom("acquire")

      runAttempt(F.bracket(F.raiseError[String](boom))(_ => F.pure(1))(_ => F.delay(released.incrementAndGet()))) shouldBe Left(boom)
      released.get shouldBe 0
    }

    "raise the release's error when use succeeded, matching guaranteeCase" in {
      val releaseError = Boom("release-only")

      runAttempt(F.bracket(F.pure("resource"))(_ => F.pure(1))(_ => F.raiseError[Unit](releaseError))) shouldBe Left(releaseError)
    }

    "let use's error win when release also fails" in {
      val useError     = Boom("use")
      val releaseError = Boom("release")

      val result = runAttempt(F.bracket(F.pure("resource"))(_ => F.raiseError[Int](useError))(_ => F.raiseError[Unit](releaseError)))

      result shouldBe Left(useError)
      useError.getSuppressed.toList should contain(releaseError)
    }

    "expose the exit case to bracketCase" in {
      val observed = AtomicReference[Option[ExitCase]](None)
      val boom     = Boom("bracket-case")

      runAttempt(
        F.bracketCase(F.pure("resource"))(_ => F.raiseError[Int](boom))((_, exitCase) => F.delay(observed.set(Some(exitCase))))
      ) shouldBe Left(boom)

      observed.get shouldBe Some(ExitCase.Errored(boom))
    }
  }

  "Effect.traverse" should {
    "preserve order across chunks" in {
      val values = List(1, 2, 3, 4, 5)

      run(Effect.traverse(values)(n => F.pure(List(n, n * 10)))) shouldBe List(1, 10, 2, 20, 3, 30, 4, 40, 5, 50)
    }

    "run effects in order and stop at the first failure" in {
      val seen = AtomicReference(List.empty[Int])
      val boom = Boom("traverse")

      val result = runAttempt(
        Effect.traverse(List(1, 2, 3)): n =>
          if n == 3 then F.raiseError[List[Int]](boom)
          else F.delay { seen.updateAndGet(n :: _); List(n) }
      )

      result shouldBe Left(boom)
      seen.get.reverse shouldBe List(1, 2)
    }
  }
