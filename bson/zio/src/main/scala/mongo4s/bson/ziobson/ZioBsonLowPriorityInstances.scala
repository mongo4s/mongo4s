package mongo4s.bson.ziobson

import mongo4s.bson.{BsonDocumentCodec, BsonError}

trait ZioBsonLowPriorityInstances:

  given fromZioCodec[A](using encoder: ZioBsonEncoder[A], decoder: ZioBsonDecoder[A]): BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value => encoder.toBsonValue(value).asDocument,
      document => decoder.fromBsonValue(document).left.map(error => BsonError.fromMessage(error.toString)),
    )
