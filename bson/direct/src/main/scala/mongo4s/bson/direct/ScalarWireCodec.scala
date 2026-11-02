package mongo4s.bson.direct

import java.util.UUID
import java.time.format.DateTimeParseException
import java.time.{Instant, LocalDate, ZoneId, ZoneOffset}

import org.bson.{BsonBinary, BsonReader, BsonWriter}

import mongo4s.bson.{BsonCodec, BsonCodecSource, BsonDecoder, BsonEncoder}

trait ScalarWireCodec[A] extends WireCodec[A], BsonCodecSource[A]:
  override def imap[B](f: A => B)(g: B => A): ScalarWireCodec[B] =
    ScalarWireCodec.from(contramap(g), map(f))

  override def iemap[B](f: A => Either[String, B])(g: B => A): ScalarWireCodec[B] =
    ScalarWireCodec.from(contramap(g), emap(f))

  def toBsonEncoder: BsonEncoder[A] =
    (value: A) =>
      val writer = BsonValueCapturingWriter.acquire()
      try
        encode(writer, value)
        writer.result
      finally BsonValueCapturingWriter.release(writer)
  end toBsonEncoder

  def toBsonDecoder: BsonDecoder[A] = bson => WireValues.decode(this, bson)

  override def toBsonCodec: BsonCodec[A] = BsonCodec.from(toBsonEncoder, toBsonDecoder)

object ScalarWireCodec:
  inline def apply[A](using instance: ScalarWireCodec[A]): ScalarWireCodec[A] = instance

  lazy val instantAsEpochMillis: ScalarWireCodec[Instant]  = ScalarWireCodec[Long].imap(Instant.ofEpochMilli)(_.toEpochMilli)
  lazy val instantAsEpochSeconds: ScalarWireCodec[Instant] = ScalarWireCodec[Long].imap(Instant.ofEpochSecond)(_.getEpochSecond)
  lazy val instantAsIsoString: ScalarWireCodec[Instant]    = ScalarWireCodec[String].iemap(parsed(Instant.parse))(_.toString)

  lazy val localDateAsString: ScalarWireCodec[LocalDate] = ScalarWireCodec[String].iemap(parsed(LocalDate.parse))(_.toString)

  def localDateAsDate(zone: ZoneId = ZoneOffset.UTC): ScalarWireCodec[LocalDate] =
    ScalarWireCodec[Instant].imap(_.atZone(zone).toLocalDate)(_.atStartOfDay(zone).toInstant)

  lazy val uuidAsBinary: ScalarWireCodec[UUID] = instance((writer, value) => writer.writeBinaryData(BsonBinary(value)), ScalarWireCodec[UUID].decode)

  def byValue[A, V](values: Iterable[A])(value: A => V)(using codec: ScalarWireCodec[V]): ScalarWireCodec[A] =
    val byStored = values.iterator.map(entry => (value(entry), entry)).toMap

    require(byStored.size == values.size, s"two of ${values.mkString(", ")} are stored as the same value")

    codec.iemap(stored => byStored.get(stored).toRight(s"no case is stored as $stored"))(value)
  end byValue

  private def parsed[A](parse: CharSequence => A): String => Either[String, A] =
    raw =>
      try Right(parse(raw))
      catch case error: DateTimeParseException => Left(error.getMessage)

  def instance[A](enc: (BsonWriter, A) => Unit, dec: BsonReader => A): ScalarWireCodec[A] =
    new ScalarWireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = enc(writer, value)
      def decode(reader: BsonReader): A              = dec(reader)

  def from[A](encoder: WireEncoder[A], decoder: WireDecoder[A]): ScalarWireCodec[A] =
    new ScalarWireCodec[A]:
      def encode(writer: BsonWriter, value: A): Unit = encoder.encode(writer, value)
      def decode(reader: BsonReader): A              = decoder.decode(reader)
      override def isAbsent(value: A): Boolean       = encoder.isAbsent(value)
      override def defaultOnMissing: Option[A]       = decoder.defaultOnMissing
