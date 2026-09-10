package mongo4s

import scala.annotation.publicInBinary

import org.bson.{BsonDocument, BsonNull}

import mongo4s.bson.{BsonDecoder, BsonError}

@publicInBinary
private[mongo4s] object SelectSupport:

  def decodeField[A](document: BsonDocument, name: String, decoder: BsonDecoder[A]): Either[BsonError, Any] =
    Option(document.get(name)) match
      case Some(value) => decoder.decode(value)
      case None        => decoder.decode(BsonNull.VALUE).left.map(_ => BsonError.MissingField(name))

  def sequence(results: List[Either[BsonError, Any]]): Either[BsonError, Array[Any]] =
    val values = new Array[Any](results.size)
    var index  = 0
    var failed = Option.empty[BsonError]

    results.foreach { result =>
      result match
        case Right(value) => values(index) = value
        case Left(error)  => if failed.isEmpty then failed = Some(error)
      index += 1
    }

    failed.toLeft(values)
  end sequence
