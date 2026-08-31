package mongo4s.benchmarks

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.json.JsonCodec
import org.bson.BsonDocument

import mongo4s.bson.BsonDocumentCodec
import mongo4s.bson.calypso.{CalypsoDecoder, CalypsoEncoder}
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}
import mongo4s.bson.medeia.MedeiaDocumentCodec

import io.circe.generic.auto.given

object CodecBenchmark:

  final case class Address(city: String, zip: String) derives MedeiaDocumentCodec, JsonCodec, WireCodec
  final case class Person(
      id: String,
      name: String,
      age: Int,
      score: Double,
      active: Boolean,
      tags: List[String],
      address: Address,
  ) derives MedeiaDocumentCodec, JsonCodec, WireCodec

  given CalypsoEncoder[Address] = CalypsoEncoder.forProduct2("city", "zip")(a => (a.city, a.zip))
  given CalypsoDecoder[Address] = CalypsoDecoder.forProduct2("city", "zip")((city, zip) => Address(city, zip))

  given CalypsoEncoder[Person] =
    CalypsoEncoder.forProduct7("id", "name", "age", "score", "active", "tags", "address")(p => (p.id, p.name, p.age, p.score, p.active, p.tags, p.address))
  given CalypsoDecoder[Person] = CalypsoDecoder.forProduct7("id", "name", "age", "score", "active", "tags", "address")((id, name, age, score, active, tags, address) =>
    Person(id, name, age, score, active, tags, address)
  )

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

  private def directCodec: BsonDocumentCodec[Person] = DocumentCodecBridge.toDocumentCodec[Person]

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
class CodecBenchmark:
  import CodecBenchmark.*

  private val person = Person("evt-1", "bob", 30, 9.5, active = true, List("a", "b", "c"), Address("NYC", "10001"))

  private val medeia    = medeiaCodec
  private val calypso   = calypsoCodec
  private val zioBsonDc = zioBsonCodec
  private val direct    = directCodec

  private val circeEncoder = mongo4cats.circe.deriveJsonBsonValueEncoder[Person]
  private val circeDecoder = mongo4cats.circe.deriveJsonBsonValueDecoder[Person]

  private val zioEncoder = mongo4cats.zio.json.deriveJsonBsonValueEncoder[Person]
  private val zioDecoder = mongo4cats.zio.json.deriveJsonBsonValueDecoder[Person]

  private val medeiaDoc: BsonDocument                = medeia.encodeDocument(person)
  private val calypsoDoc: BsonDocument               = calypso.encodeDocument(person)
  private val zioBsonDoc: BsonDocument               = zioBsonDc.encodeDocument(person)
  private val directDoc: BsonDocument                = direct.encodeDocument(person)
  private val circeBson: mongo4cats.bson.BsonValue   = circeEncoder.encode(person)
  private val zioJsonBson: mongo4cats.bson.BsonValue = zioEncoder.encode(person)

  @Benchmark def medeiaEncode: BsonDocument      = medeia.encodeDocument(person)
  @Benchmark def medeiaDecode: Either[?, Person] = medeia.decodeDocument(medeiaDoc)

  @Benchmark def calypsoEncode: BsonDocument      = calypso.encodeDocument(person)
  @Benchmark def calypsoDecode: Either[?, Person] = calypso.decodeDocument(calypsoDoc)

  @Benchmark def zioBsonEncode: BsonDocument      = zioBsonDc.encodeDocument(person)
  @Benchmark def zioBsonDecode: Either[?, Person] = zioBsonDc.decodeDocument(zioBsonDoc)

  @Benchmark def directEncode: BsonDocument      = direct.encodeDocument(person)
  @Benchmark def directDecode: Either[?, Person] = direct.decodeDocument(directDoc)

  @Benchmark def circeEncode: mongo4cats.bson.BsonValue = circeEncoder.encode(person)
  @Benchmark def circeDecode: Option[Person]            = circeDecoder.decode(circeBson)

  @Benchmark def zioJsonEncode: mongo4cats.bson.BsonValue = zioEncoder.encode(person)
  @Benchmark def zioJsonDecode: Option[Person]            = zioDecoder.decode(zioJsonBson)
