package mongo4s.bson.direct

import java.util.UUID

import scala.util.control.NonFatal

import org.bson.BsonString

@annotation.implicitNotFound(
  "No KeyCodec[${K}], so a Map keyed by it has no codec. A key is a document field name, so it is a string: " +
    "a ScalarWireCodec[${K}] that writes one is enough, or write a KeyCodec by hand."
)
trait KeyCodec[K]:
  def encodeKey(key: K): String
  def decodeKey(raw: String): Either[String, K]

object KeyCodec extends KeyCodecFromScalar:
  inline def apply[K](using instance: KeyCodec[K]): KeyCodec[K] = instance

  def instance[K](encode: K => String, decode: String => Either[String, K]): KeyCodec[K] =
    new KeyCodec[K]:
      def encodeKey(key: K): String                 = encode(key)
      def decodeKey(raw: String): Either[String, K] = decode(raw)

  given string: KeyCodec[String] = instance(identity, Right(_))
  given int: KeyCodec[Int]       = instance(_.toString, raw => raw.toIntOption.toRight(s"'$raw' is not an Int key"))
  given long: KeyCodec[Long]     = instance(_.toString, raw => raw.toLongOption.toRight(s"'$raw' is not a Long key"))

  given uuid: KeyCodec[UUID] = instance(
    _.toString,
    raw =>
      try Right(UUID.fromString(raw))
      catch case NonFatal(_) => Left(s"'$raw' is not a UUID key"),
  )

private[direct] trait KeyCodecFromScalar:
  given fromScalar: [K] => (codec: ScalarWireCodec[K]) => KeyCodec[K] =
    val encoder = codec.toBsonEncoder
    val decoder = codec.toBsonDecoder

    KeyCodec.instance(
      key =>
        encoder.encode(key) match
          case string: BsonString => string.getValue
          case other              =>
            throw IllegalArgumentException(s"a map key is a field name, so its codec has to write a string, and this one wrote ${other.getBsonType}")
      ,
      raw => decoder.decode(BsonString(raw)).left.map(_.message),
    )
  end fromScalar
