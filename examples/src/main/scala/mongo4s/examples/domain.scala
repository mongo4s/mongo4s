package mongo4s.examples

import java.time.Instant

import mongo4s.PrimaryKey
import mongo4s.bson.{BsonDecoder, BsonEncoder}
import mongo4s.bson.direct.ScalarWireCodec

import mongo4s.bson.BsonInstances.given

opaque type UserId = String

object UserId:
  def apply(value: String): UserId = value

  extension (id: UserId) def value: String = id

  // one wrap/unwrap pair drives both the wire codec (bson-direct, see bsonDirectCodecs.scala) and the
  // BsonEncoder Field/PrimaryKey queries need — no separate hand-written codec per typeclass
  given ScalarWireCodec[UserId] = ScalarWireCodec[String].imap(UserId.apply)(_.value)
  given BsonEncoder[UserId]     = summon[ScalarWireCodec[UserId]].toBsonEncoder
  given BsonDecoder[UserId]     = BsonDecoder[String].map(UserId.apply)

enum Role:
  case Admin, Member, Guest

// A second, differently-shaped enum, to show the pattern for the common real-world case: not a plain
// enum case, but one with a stored `value` used as the wire representation — e.g. matching an existing
// external string convention. `iemap` reports an unrecognized value as a decode failure instead of
// crashing with a MatchError.
enum UserRole(val value: String):
  case Admin  extends UserRole("admin")
  case Member extends UserRole("member")
  case Guest  extends UserRole("guest")

object UserRole:
  def from(value: String): Option[UserRole] = UserRole.values.find(_.value == value)

  given ScalarWireCodec[UserRole] =
    ScalarWireCodec[String].iemap(raw => from(raw).toRight(s"Unsupported role: $raw"))(_.value)
  given BsonEncoder[UserRole] = summon[ScalarWireCodec[UserRole]].toBsonEncoder

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
    // queryable via Field.of[User, UserRole](_.userRole).equalTo(UserRole.Admin) — see
    // RepositoryCatsBsonDirectApp for the full round trip through a real collection
    userRole: UserRole = UserRole.Member,
)

object User:
  given PrimaryKey[User, UserId] = PrimaryKey.single("id")(_.id)
