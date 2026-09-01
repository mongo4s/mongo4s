package mongo4s.bson.direct

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter}

import mongo4s.bson.{BsonDocumentCodec, BsonError}

object DocumentCodecBridge:
  def toDocumentCodec[A](using codec: WireCodec[A]): BsonDocumentCodec[A] =
    BsonDocumentCodec.make(
      value => {
        val document = BsonDocument()
        codec.encode(BsonDocumentWriter(document), value)
        document
      },
      document =>
        try Right(codec.decode(BsonDocumentReader(document)))
        catch case error: Throwable => Left(BsonError.fromThrowable(error)),
    )
