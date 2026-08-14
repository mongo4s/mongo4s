package mongo4s.examples

import mongo4s.bson.calypso.{CalypsoDecoder, CalypsoEncoder}

object calypsoCodecs:

  given CalypsoEncoder[UserId] = CalypsoEncoder[String].contramap(_.value)
  given CalypsoDecoder[UserId] = CalypsoDecoder[String].map(UserId.apply)

  given CalypsoEncoder[Role] = CalypsoEncoder[String].contramap(_.toString)
  given CalypsoDecoder[Role] = CalypsoDecoder[String].emap: value =>
    scala.util.Try(Role.valueOf(value)).toEither.left.map(_.getMessage)

  given CalypsoEncoder[Address] = CalypsoEncoder.forProduct2("city", "zip")(a => (a.city, a.zip))
  given CalypsoDecoder[Address] = CalypsoDecoder.forProduct2("city", "zip")(Address.apply)

  given CalypsoEncoder[User] = CalypsoEncoder.forProduct9("id", "name", "email", "age", "role", "address", "tags", "active", "createdAt"):
    user => (user.id, user.name, user.email, user.age, user.role, user.address, user.tags, user.active, user.createdAt)

  given CalypsoDecoder[User] =
    CalypsoDecoder.forProduct9("id", "name", "email", "age", "role", "address", "tags", "active", "createdAt")(User.apply)
