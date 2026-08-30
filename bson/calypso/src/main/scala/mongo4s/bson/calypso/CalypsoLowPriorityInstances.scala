package mongo4s.bson.calypso

import scala.deriving.Mirror

import mongo4s.bson.{BsonDocumentCodec, BsonError}

trait CalypsoLowPriorityInstances:

  given [A] => (encoder: CalypsoEncoder[A], decoder: CalypsoDecoder[A], mirror: Mirror.Of[A]) => BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value =>
        val bson = encoder(value)
        if bson.isDocument then bson.asDocument
        else throw IllegalArgumentException(s"calypso encoded a value as ${bson.getBsonType}, but a document was required")
      ,
      document => decoder(document).left.map(BsonError.fromMessage),
    )
