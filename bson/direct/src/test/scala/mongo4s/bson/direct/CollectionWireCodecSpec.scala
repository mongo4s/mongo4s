package mongo4s.bson.direct

import java.nio.ByteBuffer

import scala.collection.immutable.{ArraySeq, LazyList, ListSet, Queue, TreeSet}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput}
import org.bson.{BsonBinaryReader, BsonBinaryWriter, BsonDocument, ByteBufNIO}
import org.bson.codecs.{BsonDocumentCodec as DriverBsonDocumentCodec, DecoderContext}

object CollectionWireCodecSpec:
  final case class Foo(x: Int) derives WireCodec

  given Ordering[Foo] = Ordering.by(_.x)

  final case class MapHolder(m: Map[String, Foo]) derives WireCodec
  final case class ArrayHolder(xs: Array[Foo]) derives WireCodec
  final case class QueueHolder(xs: Queue[Foo]) derives WireCodec
  final case class ArraySeqHolder(xs: ArraySeq[Foo]) derives WireCodec
  final case class ListSetHolder(xs: ListSet[Foo]) derives WireCodec
  final case class LazyListHolder(xs: LazyList[Foo]) derives WireCodec
  final case class TreeSetHolder(xs: TreeSet[Foo]) derives WireCodec
  final case class ListHolder(xs: List[Foo]) derives WireCodec
  final case class VectorHolder(xs: Vector[Foo]) derives WireCodec

final class CollectionWireCodecSpec extends AnyWordSpec, Matchers:
  import CollectionWireCodecSpec.*

  private def bytesOf[A](value: A)(using codec: WireCodec[A]): Array[Byte] =
    val buffer = BasicOutputBuffer()
    val writer = BsonBinaryWriter(buffer)
    codec.encode(writer, value)
    writer.flush()
    buffer.toByteArray

  private def readerOf(bytes: Array[Byte]): BsonBinaryReader =
    BsonBinaryReader(ByteBufferBsonInput(ByteBufNIO(ByteBuffer.wrap(bytes))))

  private def roundTrip[A](value: A)(using codec: WireCodec[A]): A =
    codec.decode(readerOf(bytesOf(value)))

  private def documentOf[A](value: A)(using codec: WireCodec[A]): BsonDocument =
    DriverBsonDocumentCodec().decode(readerOf(bytesOf(value)), DecoderContext.builder().build())

  "WireCodec[Map[String, A]]" should {
    "encode as a real BSON document keyed by the map's own keys, not an array of pairs" in {
      val document = documentOf(MapHolder(Map("a" -> Foo(1), "b" -> Foo(2))))
      document.isDocument("m") shouldBe true
      document.getDocument("m").getDocument("a").getInt32("x").getValue shouldBe 1
      document.getDocument("m").getDocument("b").getInt32("x").getValue shouldBe 2
    }

    "round-trip, including an empty map" in {
      roundTrip(MapHolder(Map("a" -> Foo(1)))) shouldBe MapHolder(Map("a" -> Foo(1)))
      roundTrip(MapHolder(Map.empty)) shouldBe MapHolder(Map.empty)
    }
  }

  "WireCodec[Array[A]]" should {
    "encode as a real BSON array and round-trip" in {
      documentOf(ArrayHolder(Array(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(ArrayHolder(Array(Foo(1), Foo(2)))).xs.toList shouldBe List(Foo(1), Foo(2))
    }
  }

  "the generic Factory-based fallback for other Iterable collections" should {
    "round-trip Queue[A] as a real BSON array" in {
      documentOf(QueueHolder(Queue(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(QueueHolder(Queue(Foo(1), Foo(2)))) shouldBe QueueHolder(Queue(Foo(1), Foo(2)))
    }

    "round-trip ArraySeq[A] as a real BSON array" in {
      documentOf(ArraySeqHolder(ArraySeq(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(ArraySeqHolder(ArraySeq(Foo(1), Foo(2)))) shouldBe ArraySeqHolder(ArraySeq(Foo(1), Foo(2)))
    }

    "round-trip ListSet[A] as a real BSON array" in {
      documentOf(ListSetHolder(ListSet(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(ListSetHolder(ListSet(Foo(1), Foo(2)))) shouldBe ListSetHolder(ListSet(Foo(1), Foo(2)))
    }

    "round-trip LazyList[A] as a real BSON array" in {
      documentOf(LazyListHolder(LazyList(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(LazyListHolder(LazyList(Foo(1), Foo(2)))).xs.toList shouldBe List(Foo(1), Foo(2))
    }

    "round-trip TreeSet[A] (needs an Ordering) as a real BSON array" in {
      documentOf(TreeSetHolder(TreeSet(Foo(1), Foo(2)))).isArray("xs") shouldBe true
      roundTrip(TreeSetHolder(TreeSet(Foo(1), Foo(2)))) shouldBe TreeSetHolder(TreeSet(Foo(1), Foo(2)))
    }
  }

  "List/Vector's own dedicated instances" should {
    "still be the ones that resolve, not the new generic fallback" in {
      roundTrip(ListHolder(List(Foo(1), Foo(2)))) shouldBe ListHolder(List(Foo(1), Foo(2)))
      roundTrip(VectorHolder(Vector(Foo(1), Foo(2)))) shouldBe VectorHolder(Vector(Foo(1), Foo(2)))
    }
  }
