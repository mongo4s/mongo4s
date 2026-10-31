package mongo4s.bson.direct

import scala.compiletime.testing.typeChecks

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, BsonInt32, BsonString}

object StoredAnnotationSpec:

  final class Unencodable(val raw: Int)

  object Snake:
    given WireCodecConfig = WireCodecConfig.SnakeCase

    final case class Period(@stored("from_") from: Int, @stored("to_") to: Int, totalDays: Int) derives WireCodec

    final case class Runner(
        name: String,
        @notStored scratchedAt: Option[Long],
        @notStored trainer: Option[Unencodable],
        @notStored attempts: Int = 3,
    ) derives WireCodec

final class StoredAnnotationSpec extends AnyWordSpec, Matchers:
  import StoredAnnotationSpec.Unencodable
  import StoredAnnotationSpec.Snake.{Period, Runner}

  private def encode[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    val document = BsonDocument()
    codec.encode(BsonDocumentWriter(document), value)
    document

  private def decode[A](document: BsonDocument)(using codec: WireCodec[A]): A = codec.decode(BsonDocumentReader(document))

  "@stored" should {

    "store the field under the name given, verbatim, while the others follow the naming" in {
      val document = BsonDocument("from_", BsonInt32(1)).append("to_", BsonInt32(5)).append("total_days", BsonInt32(4))

      encode(Period(1, 5, 4)) shouldBe document
      decode[Period](document) shouldBe Period(1, 5, 4)
    }

    "report the stored names as the codec's fields" in {
      WireCodec[Period].asInstanceOf[FieldCodec[Period]].fieldNames.toList shouldBe List("from_", "to_", "total_days")
    }
  }

  "@notStored" should {

    "leave the field out of the document, and read it back as its default or None whatever the document holds" in {
      encode(Runner("ann", Some(1L), Some(Unencodable(1)), 7)) shouldBe BsonDocument("name", BsonString("ann"))

      val written = BsonDocument("name", BsonString("ann")).append("scratched_at", BsonInt32(9)).append("attempts", BsonInt32(9))

      decode[Runner](written) shouldBe Runner("ann", None, None, 3)
    }

    "need no codec for the field's type" in {
      typeChecks("WireCodec[mongo4s.bson.direct.StoredAnnotationSpec.Unencodable]") shouldBe false
    }

    "refuse a field it could not fill on read" in {
      typeChecks(
        "final case class Bad(name: String, @mongo4s.bson.direct.notStored count: Int) derives mongo4s.bson.direct.WireCodec"
      ) shouldBe false
      typeChecks(
        "final case class Good(name: String, @mongo4s.bson.direct.notStored count: Int = 1) derives mongo4s.bson.direct.WireCodec"
      ) shouldBe true
    }
  }

  "a name given twice" should {

    "be refused when the codec is built" in {
      final case class Clash(@stored("a") first: Int, a: Int) derives WireCodec

      an[IllegalArgumentException] should be thrownBy WireCodec[Clash]
    }
  }
