package mongo4s.bson

type DecodeResult[A] = Either[BsonError, A]
