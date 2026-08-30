package mongo4s.bson.ziobson

import scala.deriving.Mirror

import mongo4s.bson.{BsonDocumentCodec, BsonError}

trait ZioBsonLowPriorityInstances:

  given fromZioCodec[A](using
      encoder: ZioBsonEncoder[A],
      decoder: ZioBsonDecoder[A],
      mirror: Mirror.Of[A],
  ): BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      to = value => {
        val bson = encoder.toBsonValue(value)
        if bson.isDocument
        then bson.asDocument
        else throw IllegalArgumentException(s"zio-bson encoded a value as ${bson.getBsonType}, but a document was required")
      },
      from = document => decoder.fromBsonValue(document).left.map(error => BsonError.fromMessage(error.toString)),
    )
