package mongo4s.benchmarks

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import com.mongodb.MongoClientSettings
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonReader, BsonType, BsonWriter, ByteBufNIO}
import org.bson.codecs.{Codec, BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext, EncoderContext}

import mongo4cats.bson.Document as M4CDocument
import mongo4cats.bson.BsonValue as M4CBsonValue
import mongo4cats.codecs.{BsonValueCodecProvider, DocumentCodecProvider}

import mongo4s.bson.direct.WireCodec
import mongo4s.bson.BsonDocumentCodec
import mongo4s.benchmarks.CodecBenchmark.{Address, Person}

import io.circe.generic.auto.given

object WireFreeCodecBenchmark:

  private val driverDocCodec = DriverBsonDocumentCodec()
  private val encoderContext = EncoderContext.builder().build()
  private val decoderContext = DecoderContext.builder().build()

  private val defaultRegistry = MongoClientSettings.getDefaultCodecRegistry

  private val bsonValueCodec: Codec[M4CBsonValue] = BsonValueCodecProvider.get(classOf[M4CBsonValue], defaultRegistry)
  private val documentCodec: Codec[M4CDocument]   = DocumentCodecProvider.get(classOf[M4CDocument], defaultRegistry)

  private def medeiaCodec: BsonDocumentCodec[Person] =
    import mongo4s.bson.medeia.MedeiaInstances.given
    summon[BsonDocumentCodec[Person]]

  private def calypsoCodec: BsonDocumentCodec[Person] =
    import mongo4s.bson.calypso.CalypsoInstances.given
    summon[BsonDocumentCodec[Person]]

  private def zioBsonCodec: BsonDocumentCodec[Person] =
    import ZioSchemaBsonCodecs.given
    import mongo4s.bson.ziobson.ZioBsonInstances.given
    summon[BsonDocumentCodec[Person]]

  private def encodeHandWritten(writer: BsonWriter, p: Person): Unit =
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

  private def decodeHandWritten(reader: BsonReader): Person =
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
        case "id"      => id = reader.readString()
        case "name"    => name = reader.readString()
        case "age"     => age = reader.readInt32()
        case "score"   => score = reader.readDouble()
        case "active"  => active = reader.readBoolean()
        case "tags"    =>
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
        case _         => reader.skipValue()
    reader.readEndDocument()
    Person(id, name, age, score, active, tags, Address(city, zip))
  end decodeHandWritten

  private def toBytes(write: BsonWriter => Unit): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    write(writer)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private def viaDocument(codec: BsonDocumentCodec[Person], p: Person): Array[Byte] =
    toBytes(w => driverDocCodec.encode(w, codec.encodeDocument(p), encoderContext))

  private def fromDocument(codec: BsonDocumentCodec[Person], bytes: Array[Byte]): Person =
    codec.decodeDocument(driverDocCodec.decode(readerOf(bytes), decoderContext)) match
      case Right(p)  => p
      case Left(err) => throw err.toThrowable

  private def viaBsonValue(encode: Person => M4CBsonValue, p: Person): Array[Byte] =
    toBytes(w => bsonValueCodec.encode(w, encode(p), encoderContext))

  private def fromBsonValue(decode: M4CBsonValue => Option[Person], bytes: Array[Byte]): Person =
    decode(M4CBsonValue.document(documentCodec.decode(readerOf(bytes), decoderContext))).get

end WireFreeCodecBenchmark

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class WireFreeCodecBenchmark:
  import WireFreeCodecBenchmark.*

  private val person = Person("evt-1", "bob", 30, 9.5, active = true, List("a", "b", "c"), Address("NYC", "10001"))

  private val wire    = summon[WireCodec[Person]]
  private val medeia  = medeiaCodec
  private val calypso = calypsoCodec
  private val zioBson = zioBsonCodec

  private val circeEncoder   = mongo4cats.circe.deriveJsonBsonValueEncoder[Person]
  private val circeDecoder   = mongo4cats.circe.deriveJsonBsonValueDecoder[Person]
  private val zioJsonEncoder = mongo4cats.zio.json.deriveJsonBsonValueEncoder[Person]
  private val zioJsonDecoder = mongo4cats.zio.json.deriveJsonBsonValueDecoder[Person]

  private val wireBytes        = toBytes(w => wire.encode(w, person))
  private val handWrittenBytes = toBytes(w => encodeHandWritten(w, person))
  private val medeiaBytes      = viaDocument(medeia, person)
  private val calypsoBytes     = viaDocument(calypso, person)
  private val zioBsonBytes     = viaDocument(zioBson, person)
  private val circeBytes       = viaBsonValue(circeEncoder.encode, person)
  private val zioJsonBytes     = viaBsonValue(zioJsonEncoder.encode, person)

  @Benchmark def wireEncode: Array[Byte] = toBytes(w => wire.encode(w, person))
  @Benchmark def wireDecode: Person      = wire.decode(readerOf(wireBytes))

  @Benchmark def handWrittenEncode: Array[Byte] = toBytes(w => encodeHandWritten(w, person))
  @Benchmark def handWrittenDecode: Person      = decodeHandWritten(readerOf(handWrittenBytes))

  @Benchmark def medeiaEncode: Array[Byte] = viaDocument(medeia, person)
  @Benchmark def medeiaDecode: Person      = fromDocument(medeia, medeiaBytes)

  @Benchmark def calypsoEncode: Array[Byte] = viaDocument(calypso, person)
  @Benchmark def calypsoDecode: Person      = fromDocument(calypso, calypsoBytes)

  @Benchmark def zioBsonEncode: Array[Byte] = viaDocument(zioBson, person)
  @Benchmark def zioBsonDecode: Person      = fromDocument(zioBson, zioBsonBytes)

  @Benchmark def circeEncode: Array[Byte] = viaBsonValue(circeEncoder.encode, person)
  @Benchmark def circeDecode: Person      = fromBsonValue(circeDecoder.decode, circeBytes)

  @Benchmark def zioJsonEncode: Array[Byte] = viaBsonValue(zioJsonEncoder.encode, person)
  @Benchmark def zioJsonDecode: Person      = fromBsonValue(zioJsonDecoder.decode, zioJsonBytes)
