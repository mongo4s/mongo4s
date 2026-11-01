package mongo4s

import org.bson.types.ObjectId
import org.bson.{BsonObjectId, BsonReader, BsonType, BsonWriter}

import mongo4s.bson.*
import mongo4s.bson.direct.{FieldCodec, WireCodec, WirePath}

final case class WithId[+Id, +E](id: Id, entity: E)

object WithId:
  type Oid[E] = WithId[ObjectId, E]

  def field[Id, E, A](inner: Field[E, A]): Field[WithId[Id, E], A] = Field(inner.path)
  def idField[Id, E]: Field[WithId[Id, E], Id]                     = Field(FieldPath.literal(BsonField.Id))

  given [E] => PrimaryKey[WithId[ObjectId, E], ObjectId] =
    PrimaryKey.make(
      keyOf = _.id,
      names = List(BsonField.Id),
      fieldsOf = id => KeyFields.one(BsonField.Id, BsonObjectId(id)),
    )

  given keyedBy: [Id, E] => (encoder: BsonEncoder[Id]) => PrimaryKey[WithId[Id, E], Id] = PrimaryKey.storedId(_.id)

  given [Id, E] => (idEncoder: BsonEncoder[Id], idDecoder: BsonDecoder[Id], codec: BsonDocumentCodec[E]) => BsonDocumentCodec[WithId[Id, E]] =
    BsonDocumentCodec.from(
      BsonDocumentEncoder.instance { value =>
        val document = codec.encodeDocument(value.entity)
        document.append(BsonField.Id, idEncoder.encode(value.id))
      },
      BsonDocumentDecoder.instance { document =>
        for
          rawId  <- Option(document.get(BsonField.Id)).toRight(BsonError.MissingField(BsonField.Id))
          id     <- idDecoder.decode(rawId)
          entity <- codec.decodeDocument(document)
        yield WithId(id, entity)
      },
    )

  given wire: [Id, E] => (id: WireCodec[Id], entity: WireCodec[E]) => WireCodec[WithId[Id, E]] =
    entity match
      case fields: FieldCodec[E @unchecked] =>
        require(
          !fields.fieldNames.contains(BsonField.Id),
          "WithId writes _id itself, so the entity it wraps cannot have a field stored as _id",
        )

        new FieldCodec[WithId[Id, E]]:
          override def fieldNaming: FieldNaming  = fields.fieldNaming
          override def fieldNames: Array[String] = BsonField.Id +: fields.fieldNames

          def writeFields(writer: BsonWriter, value: WithId[Id, E]): Unit =
            writer.writeName(BsonField.Id)
            id.encode(writer, value.id)
            fields.writeFields(writer, value.entity)

          def readFields(reader: BsonReader): WithId[Id, E] =
            val start = reader.getMark
            var found = false
            var key   = null.asInstanceOf[Id]

            while !found && reader.readBsonType() != BsonType.END_OF_DOCUMENT
            do
              if reader.readName() == BsonField.Id
              then
                key = WirePath.under(BsonField.Id)(id.decode(reader))
                found = true
              else reader.skipValue()
            end while

            if !found then throw BsonError.DecodingFailure(BsonError.MissingField(BsonField.Id))

            start.reset()
            WithId(key, fields.readFields(reader))
          end readFields
      case _                                =>
        throw IllegalArgumentException("WithId needs the entity's codec to be a derived case class, whose fields it can write beside _id")
