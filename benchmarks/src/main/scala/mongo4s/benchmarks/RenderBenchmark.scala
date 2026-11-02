package mongo4s.benchmarks

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import org.bson.BsonDocument

import mongo4s.Field
import mongo4s.bson.FieldNaming
import mongo4s.bson.direct.ScalarWireCodec
import mongo4s.operations.{Filter, Update}

import mongo4s.bson.BsonInstances.given

object RenderBenchmark:
  opaque type PlayerId = String

  object PlayerId:
    def apply(value: String): PlayerId = value

    given ScalarWireCodec[PlayerId] = ScalarWireCodec.instance((writer, id) => writer.writeString(id), reader => reader.readString())

  final case class Player(id: PlayerId, name: String)
  final case class Named(id: String, name: String)

  val opaqueIds: List[PlayerId] = List.tabulate(1000)(i => PlayerId(s"player-$i"))
  val plainIds: List[String]    = List.tabulate(1000)(i => s"player-$i")

  val opaqueId: Field[Player, PlayerId] = Field.of[Player](_.id)
  val plainId: Field[Named, String]     = Field.of[Named](_.id)

  final case class Inner(homeScore: Int, awayScore: Int, periodNumber: Int)
  final case class Doc(
      eventId: Long,
      tournamentId: Long,
      homeCompetitorId: Long,
      awayCompetitorId: Long,
      scheduledAt: Long,
      isLive: Boolean,
      matchStatus: String,
      currentScore: Inner,
      lastUpdatedBy: String,
      priorityLevel: Int,
  )

  val filter: Filter[Doc] =
    Field.of[Doc](_.eventId).in(List(1L, 2L, 3L)) &&
      Field.of[Doc](_.tournamentId).equalTo(7L) &&
      Field.of[Doc](_.isLive).equalTo(true) &&
      Field.of[Doc](_.currentScore.homeScore).gte(1) &&
      Field.of[Doc](_.matchStatus).notEqualTo("ended")

  val update: Update[Doc] = Update.combine(
    Field.of[Doc](_.homeCompetitorId).set(1L),
    Field.of[Doc](_.awayCompetitorId).set(2L),
    Field.of[Doc](_.scheduledAt).set(3L),
    Field.of[Doc](_.isLive).set(true),
    Field.of[Doc](_.matchStatus).set("live"),
    Field.of[Doc](_.currentScore.homeScore).set(1),
    Field.of[Doc](_.currentScore.awayScore).set(0),
    Field.of[Doc](_.currentScore.periodNumber).set(2),
    Field.of[Doc](_.lastUpdatedBy).set("feed"),
    Field.of[Doc](_.priorityLevel).inc(1),
  )

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class RenderBenchmark:
  import RenderBenchmark.*

  @Benchmark def renderFilter: BsonDocument = filter.toBson(FieldNaming.snakeCase)
  @Benchmark def renderUpdate: BsonDocument = update.toBson(FieldNaming.snakeCase)

  @Benchmark def inThousandOpaqueIds: BsonDocument = opaqueId.in(opaqueIds).toBson(FieldNaming.identity)
  @Benchmark def inThousandStrings: BsonDocument   = plainId.in(plainIds).toBson(FieldNaming.identity)
