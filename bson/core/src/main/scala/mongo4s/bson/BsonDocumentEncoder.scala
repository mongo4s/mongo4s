package mongo4s.bson

import org.bson.{BsonDocument, BsonValue}

trait BsonDocumentEncoder[A] extends BsonEncoder[A]:
  def encodeDocument(value: A): BsonDocument

  final def encode(value: A): BsonValue = encodeDocument(value)

object BsonDocumentEncoder:
  inline def apply[A](using instance: BsonDocumentEncoder[A]): BsonDocumentEncoder[A] = instance

  def instance[A](f: A => BsonDocument): BsonDocumentEncoder[A] =
    (value: A) => f(value)
