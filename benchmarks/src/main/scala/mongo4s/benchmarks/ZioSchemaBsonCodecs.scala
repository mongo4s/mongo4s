package mongo4s.benchmarks

import zio.schema.codec.BsonSchemaCodec
import zio.schema.{DeriveSchema, Schema}
import zio.bson.{BsonDecoder, BsonEncoder}

import mongo4s.benchmarks.CodecBenchmark.{Address, Person}

trait ZioSchemaBsonCodecs:

  given Schema[Address] = DeriveSchema.gen[Address]
  given Schema[Person]  = DeriveSchema.gen[Person]

  given BsonEncoder[Address] = BsonSchemaCodec.bsonEncoder(summon[Schema[Address]])
  given BsonDecoder[Address] = BsonSchemaCodec.bsonDecoder(summon[Schema[Address]])

  given BsonEncoder[Person] = BsonSchemaCodec.bsonEncoder(summon[Schema[Person]])
  given BsonDecoder[Person] = BsonSchemaCodec.bsonDecoder(summon[Schema[Person]])

object ZioSchemaBsonCodecs extends ZioSchemaBsonCodecs
