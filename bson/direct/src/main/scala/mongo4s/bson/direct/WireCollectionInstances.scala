package mongo4s.bson.direct

import scala.reflect.ClassTag

import org.bson.{BsonReader, BsonType, BsonWriter}

import mongo4s.bson.BsonError

trait WireCollectionInstances extends WirePrimitiveInstances:

  given optionWireCodec[A](using inner: WireCodec[A]): WireCodec[Option[A]] with
    def encode(writer: BsonWriter, value: Option[A]): Unit = value match
      case Some(a) => inner.encode(writer, a)
      case None    => writer.writeNull()

    def decode(reader: BsonReader): Option[A] =
      if reader.getCurrentBsonType == BsonType.NULL then
        reader.readNull()
        None
      else Some(inner.decode(reader))

    override def isAbsent(value: Option[A]): Boolean = value.isEmpty

    override def defaultOnMissing: Option[Option[A]] = Some(None)

  given listWireCodec[A](using inner: WireCodec[A]): WireCodec[List[A]] with
    def encode(writer: BsonWriter, values: List[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): List[A] =
      reader.readStartArray()
      val builder = List.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()

  given vectorWireCodec[A](using inner: WireCodec[A]): WireCodec[Vector[A]] with
    def encode(writer: BsonWriter, values: Vector[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): Vector[A] =
      reader.readStartArray()
      val builder = Vector.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()

  given seqWireCodec[A](using inner: WireCodec[A]): WireCodec[Seq[A]] with
    def encode(writer: BsonWriter, values: Seq[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): Seq[A] =
      reader.readStartArray()
      val builder = Seq.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()

  given setWireCodec[A](using inner: WireCodec[A]): WireCodec[Set[A]] with
    def encode(writer: BsonWriter, values: Set[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): Set[A] =
      reader.readStartArray()
      val builder = Set.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()

  given mapWireCodec[A](using inner: WireCodec[A]): WireCodec[Map[String, A]] with
    def encode(writer: BsonWriter, values: Map[String, A]): Unit =
      writer.writeStartDocument()
      values.foreach: (key, value) =>
        writer.writeName(key)
        inner.encode(writer, value)
      writer.writeEndDocument()

    def decode(reader: BsonReader): Map[String, A] =
      reader.readStartDocument()
      val builder = Map.newBuilder[String, A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do
        val key = reader.readName()
        builder += key -> inner.decode(reader)
      reader.readEndDocument()
      builder.result()

  given arrayWireCodec[A: ClassTag](using inner: WireCodec[A]): WireCodec[Array[A]] with
    def encode(writer: BsonWriter, values: Array[A]): Unit =
      writer.writeStartArray()
      values.foreach(inner.encode(writer, _))
      writer.writeEndArray()

    def decode(reader: BsonReader): Array[A] =
      reader.readStartArray()
      val builder = Array.newBuilder[A]
      while reader.readBsonType() != BsonType.END_OF_DOCUMENT do builder += inner.decode(reader)
      reader.readEndArray()
      builder.result()

  given eitherWireCodec[A, B](using
      codecA: WireCodec[A],
      codecB: WireCodec[B],
      tagA: ClassTag[A],
      tagB: ClassTag[B],
  ): WireCodec[Either[A, B]] with
    private val nameA = tagA.runtimeClass.getSimpleName
    private val nameB = tagB.runtimeClass.getSimpleName
    require(
      nameA != nameB,
      s"Either's two branches must have distinguishable type names to derive a WireCodec — both were '$nameA'",
    )

    private def writeBranch[T](writer: BsonWriter, tag: String, value: T, codec: WireCodec[T]): Unit =
      writer.writeStartDocument()
      writer.writeName(WireSumDerivation.DiscriminatorField)
      writer.writeString(tag)
      codec match
        case fieldCodec: FieldCodec[T @unchecked] => fieldCodec.writeFields(writer, value)
        case scalarCodec                          =>
          writer.writeName("value")
          scalarCodec.encode(writer, value)
      writer.writeEndDocument()

    private def readBranch[T](reader: BsonReader, codec: WireCodec[T]): T = codec match
      case fieldCodec: FieldCodec[T @unchecked] => fieldCodec.readFields(reader)
      case scalarCodec                          =>
        reader.readName()
        scalarCodec.decode(reader)

    def encode(writer: BsonWriter, value: Either[A, B]): Unit = value match
      case Left(a)  => writeBranch(writer, nameA, a, codecA)
      case Right(b) => writeBranch(writer, nameB, b, codecB)

    def decode(reader: BsonReader): Either[A, B] =
      reader.readStartDocument()
      if reader.readBsonType() == BsonType.END_OF_DOCUMENT then throw BsonError.DecodingFailure(BsonError.MissingField(WireSumDerivation.DiscriminatorField))

      val firstName = reader.readName()
      if firstName != WireSumDerivation.DiscriminatorField then
        throw BsonError.DecodingFailure(
          BsonError.Custom(s"Expected discriminator field '${WireSumDerivation.DiscriminatorField}' first, got '$firstName'")
        )

      val tag    = reader.readString()
      val result =
        if tag == nameA then Left(readBranch(reader, codecA))
        else if tag == nameB then Right(readBranch(reader, codecB))
        else throw BsonError.DecodingFailure(BsonError.Custom(s"Unknown Either branch discriminator: $tag"))
      reader.readEndDocument()
      result
