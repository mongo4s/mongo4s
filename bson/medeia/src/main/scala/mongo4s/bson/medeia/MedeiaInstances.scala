package mongo4s.bson.medeia

import mongo4s.bson.{BsonDecoder, BsonEncoder}

trait MedeiaInstances extends MedeiaLowPriorityInstances:

  given fromMedeiaEncoder: [A] => (encoder: MedeiaEncoder[A]) => BsonEncoder[A] =
    (value: A) => encoder.encode(value)

  given fromMedeiaDecoder: [A] => (decoder: MedeiaDecoder[A]) => BsonDecoder[A] =
    bson => decoder.decode(bson).left.map(medeiaError)

object MedeiaInstances extends MedeiaInstances
