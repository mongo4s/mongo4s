package mongo4s.bson.ziobson

type ZioBsonEncoder[A] = zio.bson.BsonEncoder[A]
val ZioBsonEncoder = zio.bson.BsonEncoder

type ZioBsonDecoder[A] = zio.bson.BsonDecoder[A]
val ZioBsonDecoder = zio.bson.BsonDecoder
