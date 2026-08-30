package mongo4s.bson

import org.bson.BsonDocument

trait BsonDocumentCodec[A] extends BsonDocumentEncoder[A], BsonDocumentDecoder[A], BsonCodec[A]

object BsonDocumentCodec:
  inline def apply[A](using instance: BsonDocumentCodec[A]): BsonDocumentCodec[A] = instance

  def from[A](encoder: BsonDocumentEncoder[A], decoder: BsonDocumentDecoder[A]): BsonDocumentCodec[A] =
    new BsonDocumentCodec[A]:
      def encodeDocument(value: A): BsonDocument                       = encoder.encodeDocument(value)
      def decodeDocument(document: BsonDocument): Either[BsonError, A] = decoder.decodeDocument(document)

  def make[A](to: A => BsonDocument, from: BsonDocument => Either[BsonError, A]): BsonDocumentCodec[A] =
    new BsonDocumentCodec[A]:
      def encodeDocument(value: A): BsonDocument                       = to(value)
      def decodeDocument(document: BsonDocument): Either[BsonError, A] = from(document)
  
  given raw: BsonDocumentCodec[BsonDocument] = make(document => document, document => Right(document))
