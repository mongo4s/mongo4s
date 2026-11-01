package mongo4s.bson.direct

import org.bson.{BsonReader, BsonType}

import mongo4s.bson.{BsonError, BsonTypeName}

private[bson] object WireDiscriminator:

  val Field: String      = "_type"
  val ValueField: String = "value"

  def read(reader: BsonReader): String = read(reader, Field)

  def read(reader: BsonReader, field: String): String =
    val start = reader.getMark
    reader.readStartDocument()

    val firstName = if reader.readBsonType() == BsonType.END_OF_DOCUMENT then null else reader.readName()

    val tag =
      if firstName == null
      then throw BsonError.DecodingFailure(BsonError.MissingField(field))
      else if firstName == field
      then readTag(reader, field)
      else locate(reader, field)

    start.reset()
    reader.readStartDocument()
    tag
  end read

  def open(reader: BsonReader, field: String): String =
    reader.readStartDocument()
    val start = reader.getMark

    if reader.readBsonType() == BsonType.END_OF_DOCUMENT
    then throw BsonError.DecodingFailure(BsonError.MissingField(field))

    if reader.readName() == field
    then readTag(reader, field)
    else
      val tag = locate(reader, field)
      start.reset()
      tag
  end open

  def skipRest(reader: BsonReader): Unit =
    while reader.readBsonType() != BsonType.END_OF_DOCUMENT
    do
      reader.readName()
      reader.skipValue()
    end while

  def readValue[T](reader: BsonReader, codec: WireDecoder[T]): T =
    var value: Any = Unread

    while reader.readBsonType() != BsonType.END_OF_DOCUMENT
    do
      val name = reader.readName()
      if (value.asInstanceOf[AnyRef] eq Unread) && name == ValueField
      then value = WirePath.under(ValueField)(codec.decode(reader))
      else reader.skipValue()
    end while

    if value.asInstanceOf[AnyRef] eq Unread
    then throw BsonError.DecodingFailure(BsonError.MissingField(ValueField))
    else value.asInstanceOf[T]
  end readValue

  private val Unread: AnyRef = new Object

  private def readTag(reader: BsonReader, field: String): String =
    if reader.getCurrentBsonType == BsonType.STRING
    then reader.readString()
    else
      throw BsonError.DecodingFailure(
        BsonError.Nested(field, BsonError.TypeMismatch(BsonTypeName.String, BsonTypeName.of(reader.getCurrentBsonType)))
      )

  private def locate(reader: BsonReader, field: String): String =
    reader.skipValue()

    var tag: String = null
    while tag == null && reader.readBsonType() != BsonType.END_OF_DOCUMENT
    do
      if reader.readName() == field
      then tag = readTag(reader, field)
      else reader.skipValue()
    end while

    if tag == null
    then throw BsonError.DecodingFailure(BsonError.MissingField(field))
    else tag
  end locate
