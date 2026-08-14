package mongo4s

import org.bson.BsonObjectId
import org.bson.types.ObjectId

import mongo4s.bson.*

final case class WithId[+Id, +E](id: Id, entity: E)

object WithId:
  private val IdField = "_id"

  type Oid[E] = WithId[ObjectId, E]

  given [E]: PrimaryKey[WithId[ObjectId, E], ObjectId] =
    PrimaryKey.make(_.id, id => KeyFields.one(IdField, BsonObjectId(id)))

  given [Id, E](using
      idEncoder: BsonEncoder[Id],
      idDecoder: BsonDecoder[Id],
      codec: BsonDocumentCodec[E],
  ): BsonDocumentCodec[WithId[Id, E]] =
    BsonDocumentCodec.from(
      BsonDocumentEncoder.instance: value =>
        val document = codec.encodeDocument(value.entity)
        document.append(IdField, idEncoder.encode(value.id))
      ,
      BsonDocumentDecoder.instance: document =>
        for
          rawId  <- Option(document.get(IdField)).toRight(BsonError.MissingField(IdField))
          id     <- idDecoder.decode(rawId)
          entity <- codec.decodeDocument(document)
        yield WithId(id, entity),
    )
