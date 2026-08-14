package mongo4s.benchmarks

import org.bson.*
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.openjdk.jmh.annotations.*

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import mongo4s.bson.medeia.MedeiaDocumentCodec

// Research: is a hand-written, direct-to-BsonWriter/BsonReader codec (jsoniter-scala's whole
// premise, applied to BSON instead of JSON) actually faster than going through org.bson.BsonValue
// as an intermediate AST, the way medeia/calypso/zio-bson (and mongo4s itself) do today?
//
// org.bson's own Codec[T] interface (encode(BsonWriter, T, EncoderContext) / decode(BsonReader,
// DecoderContext)) is *already* AST-free and streaming - BsonWriter.writeInt32(name, value) writes
// straight to the output buffer, no BsonInt32 wrapper object, no BsonDocument (a LinkedHashMap-like
// structure) ever built. Both paths below go the full case-class-to-bytes distance so the
// comparison is fair (medeia's own codec benchmark numbers stop at BsonDocument, one step short of
// wire bytes):
//
//   medeiaFull*  - Person <-> BsonDocument (medeia's derived codec) <-> bytes (driver's own
//                  BsonDocumentCodec against a real BsonBinaryWriter/Reader)
//   direct*      - Person <-> bytes directly, hand-written against BsonWriter/BsonReader, no
//                  BsonDocument ever constructed
//
// Run with `-prof gc` for gc.alloc.rate.norm (bytes/op):
//   sbt "benchmarks/Jmh/run -prof gc WireFreeCodecBenchmark"
object WireFreeCodecBenchmark:

  final case class Address(city: String, zip: String) derives MedeiaDocumentCodec
  final case class Person(id: String, name: String, age: Int, score: Double, active: Boolean, tags: List[String], address: Address)
      derives MedeiaDocumentCodec

  private val driverDocCodec = DriverBsonDocumentCodec()

  private def medeiaCodec: mongo4s.bson.BsonDocumentCodec[Person] =
    import mongo4s.bson.medeia.MedeiaInstances.given
    summon[mongo4s.bson.BsonDocumentCodec[Person]]

  // ---- direct-to-wire: hand-written against the streaming BsonWriter/BsonReader API ----

  private def encodeDirect(writer: BsonWriter, p: Person): Unit =
    writer.writeStartDocument()
    writer.writeString("id", p.id)
    writer.writeString("name", p.name)
    writer.writeInt32("age", p.age)
    writer.writeDouble("score", p.score)
    writer.writeBoolean("active", p.active)
    writer.writeStartArray("tags")
    p.tags.foreach(writer.writeString)
    writer.writeEndArray()
    writer.writeStartDocument("address")
    writer.writeString("city", p.address.city)
    writer.writeString("zip", p.address.zip)
    writer.writeEndDocument()
    writer.writeEndDocument()

  private def decodeDirect(reader: BsonReader): Person =
    reader.readStartDocument()
    var id     = ""
    var name   = ""
    var age    = 0
    var score  = 0.0
    var active = false
    var tags   = List.empty[String]
    var city   = ""
    var zip    = ""
    while reader.readBsonType() != BsonType.END_OF_DOCUMENT do
      reader.readName() match
        case "id"     => id = reader.readString()
        case "name"   => name = reader.readString()
        case "age"    => age = reader.readInt32()
        case "score"  => score = reader.readDouble()
        case "active" => active = reader.readBoolean()
        case "tags"   =>
          reader.readStartArray()
          val builder = List.newBuilder[String]
          while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += reader.readString()
          reader.readEndArray()
          tags = builder.result()
        case "address" =>
          reader.readStartDocument()
          while reader.readBsonType() != BsonType.END_OF_DOCUMENT do
            reader.readName() match
              case "city" => city = reader.readString()
              case "zip"  => zip = reader.readString()
          reader.readEndDocument()
        case _ => reader.skipValue()
    reader.readEndDocument()
    Person(id, name, age, score, active, tags, Address(city, zip))

  private def toBytes(write: BsonWriter => Unit): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    write(writer)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  def encodeDirectFull(p: Person): Array[Byte] = toBytes(w => encodeDirect(w, p))
  def decodeDirectFull(bytes: Array[Byte]): Person = decodeDirect(readerOf(bytes))

  def encodeMedeiaFull(p: Person): Array[Byte] =
    toBytes(w => driverDocCodec.encode(w, medeiaCodec.encodeDocument(p), EncoderContext.builder().build()))

  def decodeMedeiaFull(bytes: Array[Byte]): Person =
    medeiaCodec.decodeDocument(driverDocCodec.decode(readerOf(bytes), DecoderContext.builder().build())) match
      case Right(p)  => p
      case Left(err) => throw err.toThrowable

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class WireFreeCodecBenchmark:
  import WireFreeCodecBenchmark.*

  private val person = Person("evt-1", "bob", 30, 9.5, active = true, List("a", "b", "c"), Address("NYC", "10001"))

  private val medeiaBytes = encodeMedeiaFull(person)
  private val directBytes = encodeDirectFull(person)

  @Benchmark def medeiaFullEncode: Array[Byte] = encodeMedeiaFull(person)
  @Benchmark def medeiaFullDecode: Person      = decodeMedeiaFull(medeiaBytes)

  @Benchmark def directEncode: Array[Byte] = encodeDirectFull(person)
  @Benchmark def directDecode: Person      = decodeDirectFull(directBytes)
