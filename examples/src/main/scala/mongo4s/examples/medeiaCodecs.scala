package mongo4s.examples

import mongo4s.bson.medeia.{MedeiaDecoder, MedeiaDocumentCodec, MedeiaEncoder}

object medeiaCodecs:

  given MedeiaEncoder[UserId] = MedeiaEncoder[String].contramap(_.value)
  given MedeiaDecoder[UserId] = MedeiaDecoder[String].map(UserId.apply)

  given MedeiaEncoder[Role] = MedeiaEncoder[String].contramap(_.toString)
  given MedeiaDecoder[Role] = MedeiaDecoder[String].map(Role.valueOf)

  given MedeiaEncoder[UserRole] = MedeiaEncoder[String].contramap(_.value)
  given MedeiaDecoder[UserRole] = MedeiaDecoder[String].map(raw => UserRole.from(raw).getOrElse(throw IllegalArgumentException(s"Unsupported role: $raw")))

  given MedeiaDocumentCodec[Address] = MedeiaDocumentCodec.derived[Address]
  given MedeiaDocumentCodec[User]    = MedeiaDocumentCodec.derived[User]
