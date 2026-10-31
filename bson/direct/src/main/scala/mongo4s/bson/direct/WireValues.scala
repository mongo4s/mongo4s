package mongo4s.bson.direct

import scala.util.control.NonFatal

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, BsonValue}

import mongo4s.bson.BsonError

private[direct] object WireValues:

  private val Slot = "v"

  def encode[A](encoder: WireEncoder[A], value: A): BsonValue =
    val document = BsonDocument()
    val writer   = BsonDocumentWriter(document)

    writer.writeStartDocument()
    writer.writeName(Slot)
    encoder.encode(writer, value)
    writer.writeEndDocument()

    document.get(Slot)
  end encode

  def decode[A](decoder: WireDecoder[A], value: BsonValue): Either[BsonError, A] =
    val reader = BsonDocumentReader(BsonDocument(Slot, value))

    try
      reader.readStartDocument()
      reader.readBsonType()
      reader.readName()
      Right(decoder.decode(reader))
    catch case NonFatal(error) => Left(BsonError.fromThrowable(error))
  end decode
