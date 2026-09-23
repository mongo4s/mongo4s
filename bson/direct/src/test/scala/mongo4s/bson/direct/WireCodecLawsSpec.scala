package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonDocument

import mongo4s.bson.BsonError

object WireCodecLawsSpec:

  final case class MappedName(value: Option[String])

  object MappedName:
    given WireCodec[MappedName] = WireCodec[Option[String]].imap(MappedName(_))(_.value)

  final case class ValidatedName(value: Option[String])

  object ValidatedName:
    given WireCodec[ValidatedName] = WireCodec[Option[String]].iemap(value => Right(ValidatedName(value)))(_.value)

  final case class RequiredName(value: String)

  object RequiredName:
    given WireCodec[RequiredName] = WireCodec[Option[String]].iemap(_.toRight("a name is required").map(RequiredName(_)))(name => Some(name.value))

  final case class Mapped(name: MappedName) derives WireCodec
  final case class Validated(name: ValidatedName) derives WireCodec
  final case class Required(name: RequiredName) derives WireCodec

final class WireCodecLawsSpec extends AnyWordSpec, Matchers:
  import WireCodecLawsSpec.*

  private def encode[A: WireCodec](value: A): BsonDocument = DocumentCodecBridge.toDocumentCodec[A].encodeDocument(value)

  private def decode[A: WireCodec](document: BsonDocument): Either[BsonError, A] = DocumentCodecBridge.toDocumentCodec[A].decodeDocument(document)

  "a codec mapped over Option" should {
    "read back the field it left out, through imap" in {
      val absent = Mapped(MappedName(None))

      encode(absent) shouldBe BsonDocument()
      decode[Mapped](encode(absent)) shouldBe Right(absent)
    }

    "read back the field it left out, through iemap" in {
      val absent = Validated(ValidatedName(None))

      encode(absent) shouldBe BsonDocument()
      decode[Validated](encode(absent)) shouldBe Right(absent)
    }

    "still report a missing field when the validation rejects what a missing field would read as" in {
      decode[Required](BsonDocument()).isLeft shouldBe true
      decode[Required](encode(Required(RequiredName("Ada")))) shouldBe Right(Required(RequiredName("Ada")))
    }
  }
