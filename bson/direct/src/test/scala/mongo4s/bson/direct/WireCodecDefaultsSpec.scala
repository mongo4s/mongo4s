package mongo4s.bson.direct

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.{BsonDocument, BsonInt32, BsonNull, BsonString}

import mongo4s.bson.BsonError

object WireCodecDefaultsSpec:

  object Strict:
    final case class Account(name: String, status: String = "active") derives WireCodec

  object Lenient:
    given WireCodecConfig = WireCodecConfig.Default.withDecodeMissingAsDefault(true)

    val issued = AtomicInteger(0)

    final case class Account(name: String, status: String = "active", nick: Option[String], since: Option[Int] = Some(2020)) derives WireCodec

    final case class Ticket(name: String, number: Int = issued.incrementAndGet()) derives WireCodec

    final case class Box[A](value: A, label: String = "box") derives WireCodec

    sealed trait Event derives WireCodec

    object Event:
      final case class Created(name: String, version: Int = 1) extends Event derives WireCodec

    enum Shape derives WireCodec:
      case Circle(radius: Int = 1)

    final case class Required(name: String, age: Int) derives WireCodec

  final class Nested:
    given WireCodecConfig = WireCodecConfig.Default.withDecodeMissingAsDefault(true)

    final case class Inner(name: String, size: Int = 7) derives WireCodec

final class WireCodecDefaultsSpec extends AnyWordSpec, Matchers:
  import WireCodecDefaultsSpec.*

  private def decode[A: WireCodec](document: BsonDocument): Either[BsonError, A] = DocumentCodecBridge.toDocumentCodec[A].decodeDocument(document)

  private def encode[A: WireCodec](value: A): BsonDocument = DocumentCodecBridge.toDocumentCodec[A].encodeDocument(value)

  private val named = BsonDocument("name", BsonString("ann"))

  "the default configuration" should {

    "still refuse a document missing a field, even one the case class gives a default" in {
      decode[Strict.Account](named) shouldBe Left(BsonError.MissingField("status"))
    }
  }

  "decodeMissingAsDefault" should {

    "be off unless asked for" in {
      WireCodecConfig.Default.decodeMissingAsDefault shouldBe false
      WireCodecConfig.SnakeCase.decodeMissingAsDefault shouldBe false
    }

    "fill a missing field from the case class default" in {
      decode[Lenient.Account](named) shouldBe Right(Lenient.Account("ann", "active", None, Some(2020)))
    }

    "prefer what the document holds over the default" in {
      val stored = named.clone().append("status", BsonString("blocked")).append("since", BsonInt32(1999))

      decode[Lenient.Account](stored) shouldBe Right(Lenient.Account("ann", "blocked", None, Some(1999)))
    }

    "give an Option with a default its default rather than None when the field is missing" in {
      decode[Lenient.Account](named).map(_.since) shouldBe Right(Some(2020))
    }

    "still read an explicit null into None, because a null is a value and not a missing field" in {
      decode[Lenient.Account](named.clone().append("since", BsonNull.VALUE)).map(_.since) shouldBe Right(None)
    }

    "leave an Option without a default as None" in {
      decode[Lenient.Account](named).map(_.nick) shouldBe Right(None)
    }

    "still refuse a missing field that has no default" in {
      decode[Lenient.Required](named) shouldBe Left(BsonError.MissingField("age"))
    }

    "evaluate the default each time, as the constructor would" in {
      val first  = decode[Lenient.Ticket](named).map(_.number)
      val second = decode[Lenient.Ticket](named).map(_.number)

      first should not be second
    }

    "fill the default of a generic case class" in {
      decode[Lenient.Box[Int]](BsonDocument("value", BsonInt32(3))) shouldBe Right(Lenient.Box(3, "box"))
    }

    "fill the defaults of a sealed trait's case and of an enum case" in {
      decode[Lenient.Event](BsonDocument("_type", BsonString("Created")).append("name", BsonString("a"))) shouldBe Right(Lenient.Event.Created("a", 1))
      decode[Lenient.Shape](BsonDocument("_type", BsonString("Circle"))) shouldBe Right(Lenient.Shape.Circle(1))
    }

    "fill the default of a case class nested in a class" in {
      val nested = Nested()
      decode[nested.Inner](named) shouldBe Right(nested.Inner("ann", 7))
    }

    "leave encoding alone, writing the default like any other value" in {
      encode(Lenient.Account("ann", nick = None)) shouldBe named.clone().append("status", BsonString("active")).append("since", BsonInt32(2020))
    }
  }
