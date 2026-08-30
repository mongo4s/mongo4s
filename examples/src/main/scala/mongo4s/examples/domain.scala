package mongo4s.examples

import java.time.Instant

import mongo4s.PrimaryKey
import mongo4s.bson.direct.ScalarWireCodec
import mongo4s.bson.{BsonDecoder, BsonEncoder}

import mongo4s.bson.BsonInstances.given

opaque type UserId = String

object UserId:
  def apply(value: String): UserId = value

  extension (id: UserId) def value: String = id

  given ScalarWireCodec[UserId] = ScalarWireCodec[String].imap(UserId.apply)(_.value)
  given BsonEncoder[UserId]     = summon[ScalarWireCodec[UserId]].toBsonEncoder
  given BsonDecoder[UserId]     = BsonDecoder[String].map(UserId.apply)

enum Role:
  case Admin, Member, Guest

enum UserRole(val value: String):
  case Admin  extends UserRole("admin")
  case Member extends UserRole("member")
  case Guest  extends UserRole("guest")

object UserRole:
  def from(value: String): Option[UserRole] = UserRole.values.find(_.value == value)

  given ScalarWireCodec[UserRole] = ScalarWireCodec[String].iemap(raw => from(raw).toRight(s"Unsupported role: $raw"))(_.value)
  given BsonEncoder[UserRole]     = summon[ScalarWireCodec[UserRole]].toBsonEncoder

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
    userRole: UserRole = UserRole.Member,
)

object User:
  given PrimaryKey[User, UserId] = PrimaryKey.single("id")(_.id)
