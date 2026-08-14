package mongo4s.bson

import org.bson.{BsonDocument, BsonValue}

trait BsonDocumentDecoder[A] extends BsonDecoder[A]:
  def decodeDocument(document: BsonDocument): Either[BsonError, A]

  final def decode(bson: BsonValue): Either[BsonError, A] =
    if bson.isDocument then decodeDocument(bson.asDocument)
    else Left(BsonError.typeMismatch(BsonTypeName.Object, bson))

object BsonDocumentDecoder:
  inline def apply[A](using instance: BsonDocumentDecoder[A]): BsonDocumentDecoder[A] = instance

  def instance[A](f: BsonDocument => Either[BsonError, A]): BsonDocumentDecoder[A] =
    (document: BsonDocument) => f(document)
