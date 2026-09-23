package mongo4s.bson.direct

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.BsonDocument

import mongo4s.bson.BsonError

import scala.jdk.CollectionConverters.given

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

  object Snake:
    given WireCodecConfig = WireCodecConfig.SnakeCase

    final case class Person(firstName: String) derives WireCodec

    enum Event derives WireCodec:
      case Created(firstName: String)

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

  "the naming a codec reports" should {
    "be the spelling it writes, for a product, a sum, and a product mapped with imap or iemap" in {
      val person    = WireCodec[Snake.Person]
      val event     = WireCodec[Snake.Event]
      val mapped    = person.imap[Snake.Person](identity)(identity)
      val validated = person.iemap[Snake.Person](Right(_))(identity)

      List(person, mapped, validated).foreach { codec =>
        codec.fieldNaming("firstName") shouldBe "first_name"
        written(Snake.Person("Ada"))(using codec) shouldBe List("first_name")
      }

      event.fieldNaming("firstName") shouldBe "first_name"
      written[Snake.Event](Snake.Event.Created("Ada")) shouldBe List("first_name")
    }
  }

  "a codec bridged to a document codec" should {
    "report an ordinary decoding error as a Left, but let an interruption through rather than swallow it" in {
      val broken      = WireCodec.instance[String]((writer, value) => writer.writeString(value), _ => throw IllegalStateException("broken"))
      val interrupted = WireCodec.instance[String]((writer, value) => writer.writeString(value), _ => throw InterruptedException("stop"))

      DocumentCodecBridge.toDocumentCodec[String](using broken).decodeDocument(BsonDocument()).isLeft shouldBe true
      intercept[InterruptedException](DocumentCodecBridge.toDocumentCodec[String](using interrupted).decodeDocument(BsonDocument()))
    }
  }

  private def written[A: WireCodec](value: A): List[String] =
    encode(value).keySet.asScala.toList.filterNot(_ == WireSumDerivation.DiscriminatorField)
