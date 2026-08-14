package mongo4s.bson.calypso

type CalypsoEncoder[A] = ru.m2.calypso.Encoder[A]
val CalypsoEncoder = ru.m2.calypso.Encoder

type CalypsoDecoder[A] = ru.m2.calypso.Decoder[A]
val CalypsoDecoder = ru.m2.calypso.Decoder
