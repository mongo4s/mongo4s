package mongo4s.examples

import mongo4s.bson.direct.{ScalarWireCodec, WireCodec}

object bsonDirectCodecs:

  given ScalarWireCodec[Role] = ScalarWireCodec[String].imap(Role.valueOf)(_.toString)

  given WireCodec[Address] = WireCodec.derived
  given WireCodec[User]    = WireCodec.derived
