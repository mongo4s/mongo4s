package mongo4s.examples

import mongo4s.bson.direct.WireCodec

import mongo4s.bson.BsonInstances.given

object bsonDirectCodecs:

  given WireCodec[UserId] = WireCodec.instance(
    (writer, id) => writer.writeString(id.value),
    reader => UserId(reader.readString()),
  )

  given WireCodec[Role] = WireCodec.instance(
    (writer, role) => writer.writeString(role.toString),
    reader => Role.valueOf(reader.readString()),
  )

  given WireCodec[Address] = WireCodec.derived
  given WireCodec[User]    = WireCodec.derived
