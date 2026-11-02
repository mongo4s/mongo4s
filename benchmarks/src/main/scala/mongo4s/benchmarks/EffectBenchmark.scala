package mongo4s.benchmarks

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import kyo.{AllowUnsafe, KyoApp, Sync}

import mongo4s.{Effect, ExitCase}
import mongo4s.kyo.KIO

object EffectBenchmark:
  val Steps: List[Int] = List.range(0, 1000)

  def flatMaps[F[*]](using F: Effect[F]): F[Int] =
    Steps.foldLeft(F.pure(0))((acc, _) => F.flatMap(acc)(n => F.pure(n + 1)))

  def brackets[F[*]](using F: Effect[F]): F[Unit] =
    Effect.traverse_(Steps)(i => F.void(F.bracket(F.pure(i))(F.pure)(_ => F.unit)))

  def guarantees[F[*]](using F: Effect[F]): F[Unit] =
    Effect.traverse_(Steps)(i => F.void(F.guaranteeCase(F.pure(i))((_: ExitCase) => F.unit)))

  private val catsRuntime = IORuntime.global

  def runCats[A](io: IO[A]): A = io.unsafeRunSync()(using catsRuntime)

  def runZio[A](task: zio.Task[A]): A =
    zio.Unsafe.unsafe(unsafe ?=> zio.Runtime.default.unsafe.run(task).getOrThrow())

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  def runKyo[A](io: KIO[A]): A = Sync.Unsafe.evalOrThrow(KyoApp.runAndBlock(kyo.Duration.Infinity)(io))

  def runRapid[A](task: rapid.Task[A]): A = task.sync()

  val catsEffect: Effect[IO] =
    import mongo4s.cats.CatsInstances.given
    summon[Effect[IO]]

  val zioEffect: Effect[zio.Task] =
    import mongo4s.zio.ZioInstances.given
    summon[Effect[zio.Task]]

  val kyoEffect: Effect[KIO] =
    import mongo4s.kyo.KyoInstances.given
    summon[Effect[KIO]]

  val rapidEffect: Effect[rapid.Task] =
    import mongo4s.rapid.RapidInstances.given
    summon[Effect[rapid.Task]]

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class EffectBenchmark:
  import EffectBenchmark.*

  @Benchmark def catsFlatMaps: Int  = runCats(flatMaps(using catsEffect))
  @Benchmark def zioFlatMaps: Int   = runZio(flatMaps(using zioEffect))
  @Benchmark def kyoFlatMaps: Int   = runKyo(flatMaps(using kyoEffect))
  @Benchmark def rapidFlatMaps: Int = runRapid(flatMaps(using rapidEffect))

  @Benchmark def catsBrackets: Unit  = runCats(brackets(using catsEffect))
  @Benchmark def zioBrackets: Unit   = runZio(brackets(using zioEffect))
  @Benchmark def kyoBrackets: Unit   = runKyo(brackets(using kyoEffect))
  @Benchmark def rapidBrackets: Unit = runRapid(brackets(using rapidEffect))

  @Benchmark def catsGuarantees: Unit  = runCats(guarantees(using catsEffect))
  @Benchmark def zioGuarantees: Unit   = runZio(guarantees(using zioEffect))
  @Benchmark def kyoGuarantees: Unit   = runKyo(guarantees(using kyoEffect))
  @Benchmark def rapidGuarantees: Unit = runRapid(guarantees(using rapidEffect))
