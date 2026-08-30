package mongo4s.benchmarks

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.openjdk.jmh.annotations.*

import cats.effect.IO
import rapid.Task as RapidTask
import zio.{Exit, Runtime, Scope as ZScope, Task, Unsafe}
import kyo.{AllowUnsafe, Duration as KyoDuration, KyoApp, Sync as KyoSync}
import mongo4cats.circe.deriveCirceCodecProvider
import mongo4cats.codecs.MongoCodecProvider
import mongo4cats.client.{GenericMongoClient, MongoClient as M4CClient}
import mongo4cats.operations.{Filter as M4CFilter, Update as M4CUpdate}
import mongo4cats.zio.ZMongoClient

import mongo4s.zio.ZioStream
import mongo4s.cats.CatsStream
import mongo4s.kyo.{KIO, KStream}
import mongo4s.rapid.RapidStream
import mongo4s.{Field, MongoClient as Mongo4sClient, Streamable}
import mongo4s.benchmarks.CodecBenchmark.{Address, Person}

import scala.concurrent.duration.given
import cats.effect.unsafe.implicits.given
import io.circe.generic.auto.given
import mongo4s.zio.ZioInstances.given
import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given
import mongo4s.kyo.KyoInstances.given
import mongo4s.rapid.RapidInstances.given
import mongo4s.bson.medeia.MedeiaInstances.given

private object RuntimeBenchmark:

  private final case class Ops(
      insertOne: () => Any,
      insertMany: () => Any,
      findOneById: () => Any,
      findOneByFilter: () => Any,
      findAll: () => Any,
      findStream: () => Any,
      updateOne: () => Any,
      deleteOne: () => Any,
      count: () => Any,
  )

  private type PolyRun[F[*]]         = [A] => F[A] => A
  private type PolyDrain[F[*], S[*]] = S[Person] => F[List[Person]]

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
class RuntimeBenchmark:
  import RuntimeBenchmark.*

  private val connectionString = "mongodb://localhost:27018"
  private val dbName           = "mongo4s_bench"
  private val datasetSize      = 200

  private given personCodecProvider: MongoCodecProvider[Person] = deriveCirceCodecProvider[Person]
  private val zioJsonCodecProvider: MongoCodecProvider[Person]  = mongo4cats.zio.json.deriveZioJsonCodecProvider[Person]

  private def seedPerson(i: Int): Person =
    Person(s"person-$i", s"person-$i", i % 50, i.toDouble, i % 2 == 0, List("a", "b"), Address("NYC", "10001"))

  private def seedData: List[Person] = (0 until datasetSize).map(seedPerson).toList

  private def freshPerson(prefix: String, counter: AtomicLong): Person =
    val n = counter.getAndIncrement()
    seedPerson(0).copy(id = s"$prefix-$n", name = s"$prefix-$n")

  private def freshBatch(prefix: String, counter: AtomicLong, size: Int = 10): List[Person] =
    List.fill(size)(freshPerson(prefix, counter))

  private def rotatingId(counter: AtomicLong): String =
    s"person-${(counter.getAndIncrement() % datasetSize).toInt}"

  // ---- mongo4s filters/update (field naming = identity, matches circe's default field names) ----
  private def idFilter(id: String) = Field.of[Person, String](_.id).equalTo(id)
  private val ageFilterM4s         = Field.of[Person, Int](_.age).equalTo(25)
  private val activeFilterM4s      = Field.of[Person, Boolean](_.active).equalTo(true)
  private val scoreUpdateM4s       = Field.of[Person, Double](_.score).set(math.random())

  // ---- mongo4cats filters/update ----
  private def idFilterM4c(id: String) = M4CFilter.eq("id", id)
  private val ageFilterM4c            = M4CFilter.eq("age", 25)
  private val activeFilterM4c         = M4CFilter.eq("active", true)
  private val scoreUpdateM4c          = M4CUpdate.set("score", math.random())

  private given AllowUnsafe = AllowUnsafe.embrace.danger

  private def catsRun[A](fa: IO[A]): A         = fa.unsafeRunSync()
  private def zioRun[A](fa: Task[A]): A        = Unsafe.unsafe(u ?=> Runtime.default.unsafe.run(fa).getOrThrow())
  private def rapidRun[A](fa: RapidTask[A]): A = fa.sync()
  private def kyoRun[A](fa: KIO[A]): A         =
    KyoSync.Unsafe.evalOrThrow(KyoApp.runAndBlock(KyoDuration.fromScala(30.seconds))(fa))

  private val polyCatsRun: PolyRun[IO]         = [A] => (fa: IO[A]) => catsRun(fa)
  private val polyZioRun: PolyRun[Task]        = [A] => (fa: Task[A]) => zioRun(fa)
  private val polyRapidRun: PolyRun[RapidTask] = [A] => (fa: RapidTask[A]) => rapidRun(fa)
  private val polyKyoRun: PolyRun[KIO]         = [A] => (fa: KIO[A]) => kyoRun(fa)

  private val polyCatsDrain: PolyDrain[IO, CatsStream[IO]]      = (s: CatsStream[IO][Person]) => s.compile.toList
  private val polyZioDrain: PolyDrain[Task, ZioStream]          = (s: ZioStream[Person]) => s.runCollect.map(_.toList)
  private val polyRapidDrain: PolyDrain[RapidTask, RapidStream] = (s: RapidStream[Person]) => s.toList
  private val polyKyoDrain: PolyDrain[KIO, KStream]             = (s: KStream[Person]) => s.run.map(_.toList)

  private def buildMongo4sOps[F[*], S[*]](
      client: Mongo4sClient[F, S],
      run: PolyRun[F],
      drain: PolyDrain[F, S],
      idPrefix: String,
  )(using Streamable[S, Person]): Ops =
    val db      = run(client.getDatabase(dbName))
    val readC   = run(db.getCollection[Person](s"${idPrefix}_read"))
    val writeC  = run(db.getCollection[Person](s"${idPrefix}_write"))
    val mutateC = run(db.getCollection[Person](s"${idPrefix}_mutate"))

    run(readC.insertMany(seedData))
    run(mutateC.insertMany(seedData))

    val writeCounter  = AtomicLong(0)
    val readIdCounter = AtomicLong(0)
    val updCounter    = AtomicLong(0)

    Ops(
      insertOne = () => run(writeC.insertOne(freshPerson(s"$idPrefix-one", writeCounter))),
      insertMany = () => run(writeC.insertMany(freshBatch(s"$idPrefix-many", writeCounter))),
      findOneById = () => run(readC.find(idFilter(rotatingId(readIdCounter))).first),
      findOneByFilter = () => run(readC.find(ageFilterM4s).first),
      findAll = () => run(readC.find(activeFilterM4s).all),
      findStream = () => run(drain(readC.find(activeFilterM4s).stream)),
      updateOne = () => run(mutateC.updateOne(idFilter(rotatingId(updCounter)), scoreUpdateM4s)),
      deleteOne = () => {
        val p = freshPerson(s"$idPrefix-del", writeCounter)
        run(writeC.insertOne(p))
        run(writeC.deleteOne(idFilter(p.id)))
      },
      count = () => run(readC.count(activeFilterM4s)),
    )

  private def buildMongo4sDirectOps[F[*], S[*]](
      client: Mongo4sClient[F, S],
      run: PolyRun[F],
      drain: PolyDrain[F, S],
      idPrefix: String,
  )(using Streamable[S, Person]): Ops =
    val db      = run(client.getDatabase(dbName))
    val readC   = run(db.getDirectCollection[Person](s"${idPrefix}_read"))
    val writeC  = run(db.getDirectCollection[Person](s"${idPrefix}_write"))
    val mutateC = run(db.getDirectCollection[Person](s"${idPrefix}_mutate"))

    run(readC.insertMany(seedData))
    run(mutateC.insertMany(seedData))

    val writeCounter  = AtomicLong(0)
    val readIdCounter = AtomicLong(0)
    val updCounter    = AtomicLong(0)

    Ops(
      insertOne = () => run(writeC.insertOne(freshPerson(s"$idPrefix-one", writeCounter))),
      insertMany = () => run(writeC.insertMany(freshBatch(s"$idPrefix-many", writeCounter))),
      findOneById = () => run(readC.find(idFilter(rotatingId(readIdCounter))).first),
      findOneByFilter = () => run(readC.find(ageFilterM4s).first),
      findAll = () => run(readC.find(activeFilterM4s).all),
      findStream = () => run(drain(readC.find(activeFilterM4s).stream)),
      updateOne = () => run(mutateC.updateOne(idFilter(rotatingId(updCounter)), scoreUpdateM4s)),
      deleteOne = () => {
        val p = freshPerson(s"$idPrefix-del", writeCounter)
        run(writeC.insertOne(p))
        run(writeC.deleteOne(idFilter(p.id)))
      },
      count = () => run(readC.count(activeFilterM4s)),
    )

  private def buildMongo4catsOps[F[*], S[*], R[*]](
      client: GenericMongoClient[F, S, R],
      run: PolyRun[F],
      drain: PolyDrain[F, S],
      idPrefix: String,
  )(using codecProvider: MongoCodecProvider[Person]): Ops =
    val db      = run(client.getDatabase(dbName))
    val readC   = run(db.getCollectionWithCodec[Person](s"${idPrefix}_read"))
    val writeC  = run(db.getCollectionWithCodec[Person](s"${idPrefix}_write"))
    val mutateC = run(db.getCollectionWithCodec[Person](s"${idPrefix}_mutate"))

    run(readC.insertMany(seedData))
    run(mutateC.insertMany(seedData))

    val writeCounter  = AtomicLong(0)
    val readIdCounter = AtomicLong(0)
    val updCounter    = AtomicLong(0)

    Ops(
      insertOne = () => run(writeC.insertOne(freshPerson(s"$idPrefix-one", writeCounter))),
      insertMany = () => run(writeC.insertMany(freshBatch(s"$idPrefix-many", writeCounter))),
      findOneById = () => run(readC.find(idFilterM4c(rotatingId(readIdCounter))).first),
      findOneByFilter = () => run(readC.find(ageFilterM4c).first),
      findAll = () => run(readC.find(activeFilterM4c).all),
      findStream = () => run(drain(readC.find(activeFilterM4c).stream)),
      updateOne = () => run(mutateC.updateOne(idFilterM4c(rotatingId(updCounter)), scoreUpdateM4c)),
      deleteOne = () => {
        val p = freshPerson(s"$idPrefix-del", writeCounter)
        run(writeC.insertOne(p))
        run(writeC.deleteOne(idFilterM4c(p.id)))
      },
      count = () => run(readC.count(activeFilterM4c)),
    )

  private val polyMongo4catsCatsDrain: PolyDrain[IO, [A] =>> fs2.Stream[IO, A]] =
    (s: fs2.Stream[IO, Person]) => s.compile.toList

  private val polyMongo4catsZioDrain: PolyDrain[Task, [A] =>> zio.stream.ZStream[Any, Throwable, A]] =
    (s: zio.stream.ZStream[Any, Throwable, Person]) => s.runCollect.map(_.toList)

  private var closers: List[() => Unit] = Nil

  private var catsOps: Ops           = scala.compiletime.uninitialized
  private var catsDirectOps: Ops     = scala.compiletime.uninitialized
  private var zioOps: Ops            = scala.compiletime.uninitialized
  private var rapidOps: Ops          = scala.compiletime.uninitialized
  private var kyoOps: Ops            = scala.compiletime.uninitialized
  private var m4cCatsOps: Ops        = scala.compiletime.uninitialized
  private var m4cCatsZioJsonOps: Ops = scala.compiletime.uninitialized
  private var m4cZioOps: Ops         = scala.compiletime.uninitialized

  @Setup(Level.Trial)
  def setup(): Unit =
    val catsClient = catsRun(Mongo4sClient.fromConnectionString[IO, CatsStream[IO]](connectionString))
    closers ::= (() => catsRun(catsClient.close))
    catsRun(catsClient.getDatabase(dbName).flatMap(_.drop))
    catsOps = buildMongo4sOps(catsClient, polyCatsRun, polyCatsDrain, "cats")
    catsDirectOps = buildMongo4sDirectOps(catsClient, polyCatsRun, polyCatsDrain, "cats_direct")

    val zioClient = zioRun(Mongo4sClient.fromConnectionString[Task, ZioStream](connectionString))
    closers ::= (() => zioRun(zioClient.close))
    zioOps = buildMongo4sOps(zioClient, polyZioRun, polyZioDrain, "zio")

    val rapidClient = Mongo4sClient.fromConnectionString[RapidTask, RapidStream](connectionString).sync()
    closers ::= (() => rapidClient.close.sync())
    rapidOps = buildMongo4sOps(rapidClient, polyRapidRun, polyRapidDrain, "rapid")

    val kyoClient = kyoRun(Mongo4sClient.fromConnectionString[KIO, KStream](connectionString))
    closers ::= (() => kyoRun(kyoClient.close))
    kyoOps = buildMongo4sOps(kyoClient, polyKyoRun, polyKyoDrain, "kyo")

    val (m4cCatsClient, closeM4cCats) = M4CClient.fromConnectionString[IO](connectionString).allocated.unsafeRunSync()
    closers ::= (() => closeM4cCats.unsafeRunSync())
    m4cCatsOps = buildMongo4catsOps(m4cCatsClient, polyCatsRun, polyMongo4catsCatsDrain, "m4c_cats")
    m4cCatsZioJsonOps = buildMongo4catsOps(m4cCatsClient, polyCatsRun, polyMongo4catsCatsDrain, "m4c_cats_ziojson")(using zioJsonCodecProvider)

    val zScope  = zioRun(ZScope.make)
    val zClient = zioRun(zScope.extend(ZMongoClient.fromConnectionString(connectionString)))
    closers ::= (() => zioRun(zScope.close(Exit.unit)))
    m4cZioOps = buildMongo4catsOps(zClient, polyZioRun, polyMongo4catsZioDrain, "m4c_zio")

  @TearDown(Level.Trial)
  def teardown(): Unit = closers.foreach(_.apply())

  // ---- cats ----
  @Benchmark def catsInsertOne(): Any       = catsOps.insertOne()
  @Benchmark def catsInsertMany(): Any      = catsOps.insertMany()
  @Benchmark def catsFindOneById(): Any     = catsOps.findOneById()
  @Benchmark def catsFindOneByFilter(): Any = catsOps.findOneByFilter()
  @Benchmark def catsFindAll(): Any         = catsOps.findAll()
  @Benchmark def catsFindStream(): Any      = catsOps.findStream()
  @Benchmark def catsUpdateOne(): Any       = catsOps.updateOne()
  @Benchmark def catsDeleteOne(): Any       = catsOps.deleteOne()
  @Benchmark def catsCount(): Any           = catsOps.count()

  // ---- cats + bson-direct (WireCodec, no BsonDocument on the hot path) ----
  @Benchmark def catsDirectInsertOne(): Any       = catsDirectOps.insertOne()
  @Benchmark def catsDirectInsertMany(): Any      = catsDirectOps.insertMany()
  @Benchmark def catsDirectFindOneById(): Any     = catsDirectOps.findOneById()
  @Benchmark def catsDirectFindOneByFilter(): Any = catsDirectOps.findOneByFilter()
  @Benchmark def catsDirectFindAll(): Any         = catsDirectOps.findAll()
  @Benchmark def catsDirectFindStream(): Any      = catsDirectOps.findStream()
  @Benchmark def catsDirectUpdateOne(): Any       = catsDirectOps.updateOne()
  @Benchmark def catsDirectDeleteOne(): Any       = catsDirectOps.deleteOne()
  @Benchmark def catsDirectCount(): Any           = catsDirectOps.count()

  // ---- zio ----
  @Benchmark def zioInsertOne(): Any       = zioOps.insertOne()
  @Benchmark def zioInsertMany(): Any      = zioOps.insertMany()
  @Benchmark def zioFindOneById(): Any     = zioOps.findOneById()
  @Benchmark def zioFindOneByFilter(): Any = zioOps.findOneByFilter()
  @Benchmark def zioFindAll(): Any         = zioOps.findAll()
  @Benchmark def zioFindStream(): Any      = zioOps.findStream()
  @Benchmark def zioUpdateOne(): Any       = zioOps.updateOne()
  @Benchmark def zioDeleteOne(): Any       = zioOps.deleteOne()
  @Benchmark def zioCount(): Any           = zioOps.count()

  // ---- rapid ----
  @Benchmark def rapidInsertOne(): Any       = rapidOps.insertOne()
  @Benchmark def rapidInsertMany(): Any      = rapidOps.insertMany()
  @Benchmark def rapidFindOneById(): Any     = rapidOps.findOneById()
  @Benchmark def rapidFindOneByFilter(): Any = rapidOps.findOneByFilter()
  @Benchmark def rapidFindAll(): Any         = rapidOps.findAll()
  @Benchmark def rapidFindStream(): Any      = rapidOps.findStream()
  @Benchmark def rapidUpdateOne(): Any       = rapidOps.updateOne()
  @Benchmark def rapidDeleteOne(): Any       = rapidOps.deleteOne()
  @Benchmark def rapidCount(): Any           = rapidOps.count()

  // ---- kyo ----
  @Benchmark def kyoInsertOne(): Any       = kyoOps.insertOne()
  @Benchmark def kyoInsertMany(): Any      = kyoOps.insertMany()
  @Benchmark def kyoFindOneById(): Any     = kyoOps.findOneById()
  @Benchmark def kyoFindOneByFilter(): Any = kyoOps.findOneByFilter()
  @Benchmark def kyoFindAll(): Any         = kyoOps.findAll()
  @Benchmark def kyoFindStream(): Any      = kyoOps.findStream()
  @Benchmark def kyoUpdateOne(): Any       = kyoOps.updateOne()
  @Benchmark def kyoDeleteOne(): Any       = kyoOps.deleteOne()
  @Benchmark def kyoCount(): Any           = kyoOps.count()

  // ---- mongo4cats (cats-effect) ----
  @Benchmark def mongo4catsCatsInsertOne(): Any       = m4cCatsOps.insertOne()
  @Benchmark def mongo4catsCatsInsertMany(): Any      = m4cCatsOps.insertMany()
  @Benchmark def mongo4catsCatsFindOneById(): Any     = m4cCatsOps.findOneById()
  @Benchmark def mongo4catsCatsFindOneByFilter(): Any = m4cCatsOps.findOneByFilter()
  @Benchmark def mongo4catsCatsFindAll(): Any         = m4cCatsOps.findAll()
  @Benchmark def mongo4catsCatsFindStream(): Any      = m4cCatsOps.findStream()
  @Benchmark def mongo4catsCatsUpdateOne(): Any       = m4cCatsOps.updateOne()
  @Benchmark def mongo4catsCatsDeleteOne(): Any       = m4cCatsOps.deleteOne()
  @Benchmark def mongo4catsCatsCount(): Any           = m4cCatsOps.count()

  // ---- mongo4cats (cats-effect) + zio-json codec (same runtime as above, different codec) ----
  @Benchmark def mongo4catsCatsZioJsonInsertOne(): Any       = m4cCatsZioJsonOps.insertOne()
  @Benchmark def mongo4catsCatsZioJsonInsertMany(): Any      = m4cCatsZioJsonOps.insertMany()
  @Benchmark def mongo4catsCatsZioJsonFindOneById(): Any     = m4cCatsZioJsonOps.findOneById()
  @Benchmark def mongo4catsCatsZioJsonFindOneByFilter(): Any = m4cCatsZioJsonOps.findOneByFilter()
  @Benchmark def mongo4catsCatsZioJsonFindAll(): Any         = m4cCatsZioJsonOps.findAll()
  @Benchmark def mongo4catsCatsZioJsonFindStream(): Any      = m4cCatsZioJsonOps.findStream()
  @Benchmark def mongo4catsCatsZioJsonUpdateOne(): Any       = m4cCatsZioJsonOps.updateOne()
  @Benchmark def mongo4catsCatsZioJsonDeleteOne(): Any       = m4cCatsZioJsonOps.deleteOne()
  @Benchmark def mongo4catsCatsZioJsonCount(): Any           = m4cCatsZioJsonOps.count()

  // ---- mongo4cats (zio) ----
  @Benchmark def mongo4catsZioInsertOne(): Any       = m4cZioOps.insertOne()
  @Benchmark def mongo4catsZioInsertMany(): Any      = m4cZioOps.insertMany()
  @Benchmark def mongo4catsZioFindOneById(): Any     = m4cZioOps.findOneById()
  @Benchmark def mongo4catsZioFindOneByFilter(): Any = m4cZioOps.findOneByFilter()
  @Benchmark def mongo4catsZioFindAll(): Any         = m4cZioOps.findAll()
  @Benchmark def mongo4catsZioFindStream(): Any      = m4cZioOps.findStream()
  @Benchmark def mongo4catsZioUpdateOne(): Any       = m4cZioOps.updateOne()
  @Benchmark def mongo4catsZioDeleteOne(): Any       = m4cZioOps.deleteOne()
  @Benchmark def mongo4catsZioCount(): Any           = m4cZioOps.count()
