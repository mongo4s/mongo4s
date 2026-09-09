package mongo4s.bson.medeia

import medeia.decoder.BsonDecoderError

import mongo4s.bson.BsonError

type MedeiaEncoder[A] = medeia.encoder.BsonEncoder[A]
val MedeiaEncoder = medeia.encoder.BsonEncoder

type MedeiaDecoder[A] = medeia.decoder.BsonDecoder[A]
val MedeiaDecoder = medeia.decoder.BsonDecoder

type MedeiaDocumentCodec[A] = medeia.codec.BsonDocumentCodec[A]
val MedeiaDocumentCodec = medeia.codec.BsonDocumentCodec

private[medeia] def medeiaError(error: BsonDecoderError): BsonError = BsonError.fromMessage(error.toString)
