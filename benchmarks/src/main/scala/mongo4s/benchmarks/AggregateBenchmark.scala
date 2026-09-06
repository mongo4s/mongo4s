package mongo4s.benchmarks

import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations.*

import cats.effect.IO

import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}
import mongo4s.bson.BsonDocumentCodec
import mongo4s.cats.CatsStream
import mongo4s.operations.Stage
import mongo4s.{Field, MongoClient, MongoCollection}
import mongo4s.benchmarks.CodecBenchmark.{Address, Person}

import cats.effect.unsafe.implicits.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object AggregateBenchmark:

  private val Uri = "mongodb://localhost:27018"

  private def medeiaCodec: BsonDocumentCodec[Person] =
    import mongo4s.bson.medeia.MedeiaInstances.given
    summon[BsonDocumentCodec[Person]]

  private def person(i: Int): Person =
    Person(s"id-$i", s"name-$i", 20 + (i % 50), i.toDouble, active = true, List("a", "b", "c"), Address("NYC", "10001"))

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
class AggregateBenchmark:
  import AggregateBenchmark.*

  type S[A] = CatsStream[IO][A]

  @Param(Array("10", "10000"))
  var documents: Int = uninitialized

  private var client: MongoClient[IO, S]                 = uninitialized
  private var collection: MongoCollection[IO, S, Person] = uninitialized

  private val bridged: BsonDocumentCodec[Person] = DocumentCodecBridge.toDocumentCodec[Person]
  private val medeia: BsonDocumentCodec[Person]  = medeiaCodec

  private val pipeline: Seq[Stage[Person]] =
    Seq(Stage.matching(Field.of[Person, Boolean](_.active).equalTo(true)))

  @Setup(Level.Trial)
  def setUp(): Unit =
    val program =
      for
        c <- MongoClient.fromConnectionString[IO, S](Uri)
        d <- c.getDatabase("aggregate_bench")
        p <- d.getDirectCollection[Person]("people")
        _ <- p.deleteMany(Field.of[Person, Boolean](_.active).equalTo(true))
        _ <- p.insertMany(List.tabulate(documents)(person))
      yield (c, p)

    val (c, p) = program.unsafeRunSync()
    client = c
    collection = p
  end setUp

  @TearDown(Level.Trial)
  def tearDown(): Unit = client.close.unsafeRunSync()

  @Benchmark def aggregateDirect: Int =
    collection.aggregateDirect[Person](pipeline).all.unsafeRunSync().size

  @Benchmark def aggregateBridged: Int =
    collection.aggregate[Person](pipeline)(using None)(using bridged).all.unsafeRunSync().size

  @Benchmark def aggregateMedeia: Int =
    collection.aggregate[Person](pipeline)(using None)(using medeia).all.unsafeRunSync().size

end AggregateBenchmark
