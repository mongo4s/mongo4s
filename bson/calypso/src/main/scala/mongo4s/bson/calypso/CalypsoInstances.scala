package mongo4s.bson.calypso

import mongo4s.bson.{BsonDecoder, BsonEncoder, BsonError}

trait CalypsoInstances extends CalypsoLowPriorityInstances:

  given [A] => (encoder: CalypsoEncoder[A]) => BsonEncoder[A] =
    (value: A) => encoder(value)

  given [A] => (decoder: CalypsoDecoder[A]) => BsonDecoder[A] =
    (bson) => decoder(bson).left.map(BsonError.fromMessage)

object CalypsoInstances extends CalypsoInstances
