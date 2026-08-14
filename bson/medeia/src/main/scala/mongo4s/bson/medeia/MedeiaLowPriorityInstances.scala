package mongo4s.bson.medeia

import medeia.decoder.BsonDecoderError

import mongo4s.bson.{BsonDocumentCodec, BsonError}

private[medeia] def medeiaError(error: BsonDecoderError): BsonError = BsonError.fromMessage(error.toString)

trait MedeiaLowPriorityInstances:

  given fromMedeiaDocumentCodec[A](using codec: MedeiaDocumentCodec[A]): BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value => codec.encode(value).asDocument,
      document => codec.decode(document).left.map(medeiaError),
    )
