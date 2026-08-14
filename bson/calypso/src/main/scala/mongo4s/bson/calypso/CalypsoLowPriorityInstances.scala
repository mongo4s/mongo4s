package mongo4s.bson.calypso

import mongo4s.bson.{BsonDocumentCodec, BsonError}

trait CalypsoLowPriorityInstances:

  given [A] => (encoder: CalypsoEncoder[A], decoder: CalypsoDecoder[A]) => BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value => encoder(value).asDocument,
      document => decoder(document).left.map(BsonError.fromMessage),
    )
