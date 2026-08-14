package mongo4s.examples

import zio.schema.codec.BsonSchemaCodec
import zio.schema.{DeriveSchema, Schema}

import mongo4s.bson.ziobson.{ZioBsonDecoder, ZioBsonEncoder}

object zioBsonCodecs:

  given Schema[UserId] = Schema[String].transform(UserId.apply, _.value)
  given Schema[Role]   = Schema[String].transform(Role.valueOf, _.toString)

  given Schema[Address] = DeriveSchema.gen[Address]
  given Schema[User]    = DeriveSchema.gen[User]

  given ZioBsonEncoder[User] = BsonSchemaCodec.bsonEncoder(summon[Schema[User]])
  given ZioBsonDecoder[User] = BsonSchemaCodec.bsonDecoder(summon[Schema[User]])
