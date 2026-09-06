package mongo4s.benchmarks

import java.nio.ByteBuffer
import java.util.UUID
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.types.ObjectId
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonReader, BsonWriter, ByteBufNIO}

import mongo4s.bson.direct.WireCodec

import mongo4s.bson.BsonInstances.given

object ScalarWireCodecBenchmark:

  final case class Audit(
      id: ObjectId,
      session: UUID,
      at: Instant,
      amount: BigDecimal,
      actor: String,
      action: String,
      version: Int,
  )

  object bridgedScalars:
    given WireCodec[ObjectId]   = WireCodec.fromBsonValueCodec[ObjectId]
    given WireCodec[UUID]       = WireCodec.fromBsonValueCodec[UUID]
    given WireCodec[Instant]    = WireCodec.fromBsonValueCodec[Instant]
    given WireCodec[BigDecimal] = WireCodec.fromBsonValueCodec[BigDecimal]

  val nativeCodec: WireCodec[Audit] = WireCodec.derived[Audit]

  val bridgedCodec: WireCodec[Audit] =
    import bridgedScalars.given
    WireCodec.derived[Audit]

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class ScalarWireCodecBenchmark:
  import ScalarWireCodecBenchmark.*

  private val audit = Audit(
    ObjectId("656b1f2c8e4a3b0012ab34cd"),
    UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
    Instant.ofEpochMilli(1_733_000_000_123L),
    BigDecimal("12345.6789"),
    "svc-billing",
    "charge.captured",
    3,
  )

  private def toBytes(write: BsonWriter => Unit): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    write(writer)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private val bytes = toBytes(w => nativeCodec.encode(w, audit))

  @Benchmark def nativeEncode: Array[Byte] = toBytes(w => nativeCodec.encode(w, audit))
  @Benchmark def nativeDecode: Audit       = nativeCodec.decode(readerOf(bytes))

  @Benchmark def bridgedEncode: Array[Byte] = toBytes(w => bridgedCodec.encode(w, audit))
  @Benchmark def bridgedDecode: Audit       = bridgedCodec.decode(readerOf(bytes))

end ScalarWireCodecBenchmark
