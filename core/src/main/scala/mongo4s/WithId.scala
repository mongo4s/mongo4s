package mongo4s

import org.bson.BsonObjectId
import org.bson.types.ObjectId

import mongo4s.bson.*

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
