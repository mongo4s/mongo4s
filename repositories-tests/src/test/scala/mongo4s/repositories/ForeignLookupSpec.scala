package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import org.bson.{BsonDocument, BsonString}

import mongo4s.Field
import mongo4s.cats.CatsStream
import mongo4s.testkit.FakeMongoCollection
import mongo4s.bson.{BsonDocumentCodec, BsonError, FieldNaming}
import mongo4s.operations.{GraphLookupOptions, Stage}

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object ForeignLookupSpec:
  final case class Order(userId: String, itemCount: Int)
  final case class Account(accountId: String, managerId: String, isActive: Boolean)

  val unused: BsonDocumentCodec[Account] = BsonDocumentCodec.make(_ => BsonDocument(), _ => Left(BsonError.Custom("not read")))

final class ForeignLookupSpec extends AnyWordSpec, Matchers:
  import ForeignLookupSpec.*

  type S[A] = CatsStream[IO][A]

  private val accounts = FakeMongoCollection[IO, S, Account](unused, name = "accounts", naming = FieldNaming.identity)

  private val userId    = Field.of[Order, String](_.userId)
  private val accountId = Field.of[Account, String](_.accountId)
  private val managerId = Field.of[Account, String](_.managerId)
  private val active    = Field.of[Account, Boolean](_.isActive)

  private def json(stage: Stage[Order]): String = stage.toBson(FieldNaming.snakeCase).toJson

  "a lookup given the foreign collection" should {

    "take the name from it and spell the foreign field through its naming, the local one through the pipeline's" in {
      json(Stage.lookup(accounts, userId, accountId, as = "account")) shouldBe
        """{"$lookup": {"from": "accounts", "localField": "user_id", "foreignField": "accountId", "as": "account"}}"""
    }

    "render the sub-pipeline over the foreign collection through the foreign naming" in {
      json(Stage.lookupWith(accounts, List(Stage.matching(active.equalTo(true))), as = "active")) shouldBe
        """{"$lookup": {"from": "accounts", "pipeline": [{"$match": {"isActive": true}}], "as": "active"}}"""

      json(Stage.lookupWith(accounts, Nil, "all", Some(BsonDocument("owner", BsonString("$user_id"))))) shouldBe
        """{"$lookup": {"from": "accounts", "let": {"owner": "$user_id"}, "pipeline": [], "as": "all"}}"""
    }

    "render a keyed lookup's foreign key and sub-pipeline through the foreign naming" in {
      json(Stage.lookupWithKeys(accounts, userId, managerId, List(Stage.matching(active.equalTo(true))), "managed")) shouldBe
        """{"$lookup": {"from": "accounts", "localField": "user_id", "foreignField": "managerId", """ +
        """"pipeline": [{"$match": {"isActive": true}}], "as": "managed"}}"""
    }

    "render a $graphLookup's connecting fields and search restriction through the foreign naming" in {
      val options = GraphLookupOptions.default[Account].withMaxDepth(2).restrictedTo(active.equalTo(true))

      json(Stage.graphLookup[Order, Account, String](accounts, BsonString("$user_id"), managerId, accountId, "chain", options)) shouldBe
        """{"$graphLookup": {"from": "accounts", "startWith": "$user_id", "connectFromField": "managerId", """ +
        """"connectToField": "accountId", "as": "chain", "maxDepth": 2, "restrictSearchWithMatch": {"isActive": true}}}"""

      json(Stage.graphLookup[Order, Account, String](accounts, BsonString("$user_id"), managerId, accountId, "chain")) shouldBe
        """{"$graphLookup": {"from": "accounts", "startWith": "$user_id", "connectFromField": "managerId", "connectToField": "accountId", "as": "chain"}}"""
    }

    "still join on matching types only" in {
      "Stage.lookup(accounts, Field.of[ForeignLookupSpec.Order, Int](_.itemCount), accountId, as = \"x\")" shouldNot typeCheck
    }
  }
