package mongo4s.benchmarks

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonReader, BsonWriter, ByteBufNIO}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4s.bson.BsonDocumentCodec
import mongo4s.bson.direct.{DocumentCodecBridge, ScalarWireCodec, WireCodec, WireCodecConfig}

object WideEntity:
  given WireCodecConfig = WireCodecConfig.SnakeCase

  enum Provider(val value: String):
    case Betradar extends Provider("betradar")
    case Lsports  extends Provider("lsports")
    case Genius   extends Provider("genius")

  object Provider:
    given ScalarWireCodec[Provider] = ScalarWireCodec.byValue(Provider.values)(_.value)

  sealed trait Status derives WireCodec

  object Status:
    case object NotStarted                          extends Status
    final case class Live(minute: Int, period: Int) extends Status derives WireCodec
    final case class Ended(reason: String)          extends Status derives WireCodec

  final case class Score(
      home: Int,
      away: Int,
      homeSet1: Option[Int],
      awaySet1: Option[Int],
      homeSet2: Option[Int],
      awaySet2: Option[Int],
      homeSet3: Option[Int],
      awaySet3: Option[Int],
      homeSet4: Option[Int],
      awaySet4: Option[Int],
      homeSet5: Option[Int],
      awaySet5: Option[Int],
      homeCorners: Option[Int],
      awayCorners: Option[Int],
      homeCards: Option[Int],
      awayCards: Option[Int],
      homePenalties: Option[Int],
      awayPenalties: Option[Int],
      homeOvertime: Option[Int],
      awayOvertime: Option[Int],
      homeAces: Option[Int],
      awayAces: Option[Int],
      homeFaults: Option[Int],
      awayFaults: Option[Int],
      homeBreaks: Option[Int],
      awayBreaks: Option[Int],
  ) derives WireCodec

  final case class Period(from: Int, to: Int) derives WireCodec

  final case class Event(
      eventId: Long,
      sportId: Int,
      categoryId: Long,
      tournamentId: Long,
      seasonId: Option[Long],
      homeCompetitorId: Long,
      awayCompetitorId: Long,
      name: String,
      localName: Option[String],
      venue: Option[String],
      scheduled: Instant,
      updatedAt: Instant,
      status: Status,
      priority: Int,
      margin: BigDecimal,
      isLive: Boolean,
      isNeutral: Boolean,
      tags: List[String],
      periods: List[Period],
      scores: Map[Provider, Score],
  ) derives WireCodec

end WideEntity

object WideEntityBenchmark:
  import WideEntity.*

  private val driverDocCodec = DriverBsonDocumentCodec()
  private val encoderContext = EncoderContext.builder().build()
  private val decoderContext = DecoderContext.builder().build()

  private def toBytes(write: BsonWriter => Unit): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    write(writer)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private val score = Score(
    home = 2,
    away = 1,
    homeSet1 = None,
    awaySet1 = Some(1),
    homeSet2 = Some(1),
    awaySet2 = None,
    homeSet3 = Some(1),
    awaySet3 = Some(1),
    homeSet4 = None,
    awaySet4 = Some(1),
    homeSet5 = Some(1),
    awaySet5 = None,
    homeCorners = Some(1),
    awayCorners = Some(1),
    homeCards = None,
    awayCards = Some(1),
    homePenalties = Some(1),
    awayPenalties = None,
    homeOvertime = Some(1),
    awayOvertime = Some(1),
    homeAces = None,
    awayAces = Some(1),
    homeFaults = Some(1),
    awayFaults = None,
    homeBreaks = Some(1),
    awayBreaks = Some(1),
  )

  val event: Event = Event(
    eventId = 42L,
    sportId = 1,
    categoryId = 7L,
    tournamentId = 17L,
    seasonId = Some(2026L),
    homeCompetitorId = 1001L,
    awayCompetitorId = 1002L,
    name = "Home FC vs Away FC",
    localName = None,
    venue = Some("Stadium"),
    scheduled = Instant.parse("2026-11-01T18:00:00Z"),
    updatedAt = Instant.parse("2026-11-01T18:45:12Z"),
    status = Status.Live(45, 1),
    priority = 3,
    margin = BigDecimal("1.0525"),
    isLive = true,
    isNeutral = false,
    tags = List("top", "derby"),
    periods = List(Period(0, 45), Period(45, 90)),
    scores = Map(Provider.Betradar -> score, Provider.Lsports -> score, Provider.Genius -> score),
  )

  private val bridged: BsonDocumentCodec[Event] = DocumentCodecBridge.toDocumentCodec[Event]

  private def viaDocument(value: Event): Array[Byte] =
    toBytes(writer => driverDocCodec.encode(writer, bridged.encodeDocument(value), encoderContext))

  private def fromDocument(bytes: Array[Byte]): Event =
    bridged.decodeDocument(driverDocCodec.decode(readerOf(bytes), decoderContext)) match
      case Right(value) => value
      case Left(error)  => throw error.toThrowable

end WideEntityBenchmark

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class WideEntityBenchmark:
  import WideEntity.*
  import WideEntityBenchmark.*

  private val wire  = summon[WireCodec[Event]]
  private val bytes = toBytes(writer => wire.encode(writer, event))

  require(wire.decode(readerOf(bytes)) == event && fromDocument(viaDocument(event)) == event, "the benchmark entity does not round-trip")

  @Benchmark def wireEncode: Array[Byte] = toBytes(writer => wire.encode(writer, event))
  @Benchmark def wireDecode: Event       = wire.decode(readerOf(bytes))

  @Benchmark def bridgedEncode: Array[Byte] = viaDocument(event)
  @Benchmark def bridgedDecode: Event       = fromDocument(bytes)
