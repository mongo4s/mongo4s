package mongo4s.examples

import java.time.Instant

import mongo4s.PrimaryKey
import mongo4s.bson.{BsonDecoder, BsonEncoder}

import mongo4s.bson.BsonInstances.given

opaque type UserId = String

object UserId:
  def apply(value: String): UserId = value

  extension (id: UserId) def value: String = id

  given BsonEncoder[UserId] = BsonEncoder[String].contramap(_.value)
  given BsonDecoder[UserId] = BsonDecoder[String].map(UserId.apply)

enum Role:
  case Admin, Member, Guest

final case class Address(city: String, zip: String)

final case class User(
    id: UserId,
    name: String,
    email: String,
    age: Int,
    role: Role,
    address: Address,
    tags: List[String],
    active: Boolean,
    createdAt: Instant,
)

object User:
  given PrimaryKey[User, UserId] = PrimaryKey.single("id")(_.id)
