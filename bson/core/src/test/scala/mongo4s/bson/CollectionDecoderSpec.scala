package mongo4s.bson

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.BsonInstances.given

import scala.jdk.CollectionConverters.given

final class CollectionDecoderSpec extends AnyWordSpec with Matchers:

  private def array(values: BsonValue*): BsonArray = BsonArray(values.asJava)

  private val mixed = array(BsonInt32(1), BsonString("a"), BsonString("b"))

  "the collection decoders" should {
    "name the first element that fails, by its index" in {
      val expected = Left(BsonError.Nested("1", BsonError.TypeMismatch(BsonTypeName.Int, BsonTypeName.String)))

      BsonDecoder[List[Int]].decode(mixed) shouldBe expected
      BsonDecoder[Vector[Int]].decode(mixed) shouldBe expected
      BsonDecoder[Seq[Int]].decode(mixed) shouldBe expected
      BsonDecoder[Set[Int]].decode(mixed) shouldBe expected
    }

    "say which element failed in the message" in {
      BsonDecoder[List[Int]].decode(mixed).left.map(_.message) shouldBe Left("1 -> Expected int but got string")
    }

    "name the path through nested arrays" in {
      val nested = array(array(BsonInt32(1)), array(BsonInt32(2), BsonString("x")))

      BsonDecoder[List[List[Int]]].decode(nested).left.map(_.message) shouldBe Left("1 -> 1 -> Expected int but got string")
    }

    "keep the order of the elements" in {
      val values = array((1 to 50).map(BsonInt32(_))*)

      BsonDecoder[List[Int]].decode(values) shouldBe Right((1 to 50).toList)
      BsonDecoder[Vector[Int]].decode(values) shouldBe Right((1 to 50).toVector)
      BsonDecoder[Seq[Int]].decode(values) shouldBe Right((1 to 50).toList)
      BsonDecoder[Set[Int]].decode(values) shouldBe Right((1 to 50).toSet)
    }

    "read an empty array" in {
      BsonDecoder[List[Int]].decode(array()) shouldBe Right(Nil)
      BsonDecoder[Vector[Int]].decode(array()) shouldBe Right(Vector.empty)
    }

    "refuse a value that is not an array" in {
      BsonDecoder[List[Int]].decode(BsonInt32(1)) shouldBe Left(BsonError.TypeMismatch(BsonTypeName.Array, BsonTypeName.Int))
      BsonDecoder[Vector[Int]].decode(BsonInt32(1)) shouldBe Left(BsonError.TypeMismatch(BsonTypeName.Array, BsonTypeName.Int))
    }
  }

  "the map decoder" should {
    "name the first entry that fails, in document order" in {
      val document = BsonDocument("a", BsonInt32(1)).append("b", BsonString("x")).append("c", BsonString("y"))

      BsonDecoder[Map[String, Int]].decode(document) shouldBe
        Left(BsonError.Nested("b", BsonError.TypeMismatch(BsonTypeName.Int, BsonTypeName.String)))
    }

    "read every entry" in {
      val document = (1 to 20).foldLeft(BsonDocument())((acc, i) => acc.append(s"k$i", BsonInt32(i)))

      BsonDecoder[Map[String, Int]].decode(document) shouldBe Right((1 to 20).map(i => s"k$i" -> i).toMap)
    }
  }
