package mongo4s.bson.medeia

import medeia.decoder.BsonDecoderError

import mongo4s.bson.BsonDocumentCodec

trait MedeiaLowPriorityInstances:

  given fromMedeiaDocumentCodec: [A] => (codec: MedeiaDocumentCodec[A]) => BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value => codec.encode(value).asDocument,
      document => codec.decode(document).left.map(medeiaError),
    )
