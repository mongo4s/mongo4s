package mongo4s.it

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import org.testcontainers.containers.MongoDBContainer

import cats.effect.IO

import mongo4s.cats.CatsStream
import mongo4s.operations.{Update, UpdateOptions}
import mongo4s.{Field, MongoClient}
import mongo4s.bson.direct.WireCodec

import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ArrayFiltersItSpec:
  final case class Item(sku: String, qty: Int) derives WireCodec
  final case class Order(id: String, items: List[Item]) derives WireCodec

final class ArrayFiltersItSpec extends AsyncWordSpec, AsyncIOSpec, Matchers, BeforeAndAfterAll:
  import ArrayFiltersItSpec.{Item, Order}

  private val container = new MongoDBContainer("mongo:7")

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit  = container.stop()

  type S[A] = CatsStream[IO][A]

  private val idField = Field.of[Order, String](_.id)

  private def seeded(name: String) =
    for
      client   <- MongoClient.fromConnectionString[IO, S](container.getConnectionString)
      database <- client.getDatabase("array_filters_it")
      orders   <- database.getDirectCollection[Order](name)
      _        <- orders.insertOne(Order("1", List(Item("a", 1), Item("b", 5))))
    yield orders

  "the all-positional operator $[]" should {
    "update every element without needing arrayFilters" in {
      val everyQty: Field[Order, Int] = Field.of[Order, List[Item]](_.items) / "$[]" / "qty"

      val program =
        for
          orders <- seeded("all_positional")
          _      <- orders.updateOne(idField.equalTo("1"), Update.set(everyQty, 9))
          found  <- orders.find(idField.equalTo("1")).first
        yield found

      program.asserting(_ shouldBe Some(Order("1", List(Item("a", 9), Item("b", 9)))))
    }
  }

  "the filtered positional operator $[identifier]" should {
    "update only the elements the array filter selects" in {
      val lowQty: Field[Order, Int]    = Field.of[Order, List[Item]](_.items) / "$[low]" / "qty"
      val elementQty: Field[Item, Int] = Field.stored("low.qty")

      val program =
        for
          orders <- seeded("filtered_positional")
          _      <- orders.updateOne(
                      idField.equalTo("1"),
                      Update.set(lowQty, 100),
                      UpdateOptions.default.withArrayFilters(Seq(elementQty.lt(3))),
                    )
          found  <- orders.find(idField.equalTo("1")).first
        yield found

      program.asserting(_ shouldBe Some(Order("1", List(Item("a", 100), Item("b", 5)))))
    }
  }
