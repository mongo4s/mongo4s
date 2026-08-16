package mongo4s.examples

import mongo4s.bson.direct.{ScalarWireCodec, WireCodec}

import mongo4s.bson.BsonInstances.given

object bsonDirectCodecs:

  // UserId is an opaque type over String — its WireCodec already comes from domain.scala's own
  // `ScalarWireCodec[String].imap(UserId.apply)(_.value)`, so there's nothing to redeclare here.

  given ScalarWireCodec[Role] = ScalarWireCodec[String].imap(Role.valueOf)(_.toString)

  given WireCodec[Address] = WireCodec.derived
  given WireCodec[User]    = WireCodec.derived
