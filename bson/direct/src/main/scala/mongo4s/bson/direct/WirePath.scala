package mongo4s.bson.direct

import scala.util.control.NonFatal

import mongo4s.bson.BsonError

private[mongo4s] object WirePath:

  inline def under[A](inline segment: String)(inline decode: A): A =
    try decode
    catch case NonFatal(error) => throw BsonError.DecodingFailure(BsonError.Nested(segment, BsonError.fromThrowable(error)))
