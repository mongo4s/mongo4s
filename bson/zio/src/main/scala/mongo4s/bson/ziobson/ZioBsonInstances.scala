package mongo4s.bson.ziobson

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError}

trait ZioBsonInstances extends ZioBsonLowPriorityInstances:

  given fromZioEncoder[A](using encoder: ZioBsonEncoder[A]): BsonEncoder[A] =
    (value: A) => encoder.toBsonValue(value)

  given fromZioDecoder[A](using decoder: ZioBsonDecoder[A]): BsonDecoder[A] =
    bson => decoder.fromBsonValue(bson).left.map(error => BsonError.fromMessage(error.toString))

object ZioBsonInstances extends ZioBsonInstances
