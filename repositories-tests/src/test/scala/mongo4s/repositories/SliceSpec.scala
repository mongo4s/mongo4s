package mongo4s.repositories

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import cats.effect.IO
import cats.effect.unsafe.implicits.given
import org.bson.{BsonArray, BsonDocument, BsonString}

import scala.jdk.CollectionConverters.given

import mongo4s.bson.*
import mongo4s.cats.CatsStream
import mongo4s.operations.{Projection, Sort}
import mongo4s.testkit.FakeMongoCollection
import mongo4s.Field

import mongo4s.bson.BsonInstances.given
import mongo4s.cats.CatsInstances.given

object SliceSpec:
  final case class Thread(id: String, comments: List[String])

  object Thread:
    given BsonDocumentCodec[Thread] = BsonDocumentCodec.make(
      thread =>
        BsonDocument()
          .append("id", BsonString(thread.id))
          .append("comments", BsonArray(thread.comments.map(c => BsonString(c): org.bson.BsonValue).asJava)),
      document =>
        for
          id       <- Option(document.get("id")).toRight(BsonError.MissingField("id")).flatMap(BsonDecoder[String].decode)
          comments <- Option(document.get("comments"))
                        .toRight(BsonError.MissingField("comments"))
                        .flatMap(BsonDecoder[List[String]].decode)
        yield Thread(id, comments),
    )

final class SliceSpec extends AnyWordSpec, Matchers:
  import SliceSpec.Thread
  import SliceSpec.Thread.given

  type S[A] = CatsStream[IO][A]

  private val id       = Field.of[Thread, String](_.id)
  private val comments = Field.of[Thread, List[String]](_.comments)

  private val thread = Thread("1", List("a", "b", "c", "d", "e"))

  private def collection: FakeMongoCollection[IO, S, Thread] =
    val fake = FakeMongoCollection[IO, S, Thread](summon[BsonDocumentCodec[Thread]], _ => fs2.Stream.empty)
    fake.insertMany(List(thread, Thread("2", List("x", "y")))).unsafeRunSync()
    fake

  private def projected(projection: Projection[Thread]): List[String] =
    collection.find(id.equalTo("1")).projection(projection).all.unsafeRunSync().head.comments

  "the fake's $slice" should {

    "take the first elements" in {
      projected(Projection.empty[Thread].slice(comments, 2)) shouldBe List("a", "b")
    }

    "take the last elements when the count is negative" in {
      projected(Projection.empty[Thread].slice(comments, -2)) shouldBe List("d", "e")
    }

    "take a window from an offset" in {
      projected(Projection.empty[Thread].sliceFrom(comments, skip = 1, count = 2)) shouldBe List("b", "c")
    }

    "leave the rest of the document alone" in {
      collection.find(id.equalTo("1")).projection(Projection.empty[Thread].slice(comments, 1)).all.unsafeRunSync().head.id shouldBe "1"
    }

    "apply after an inclusion, not instead of it" in {
      projected(Projection.empty[Thread].include(id).include(comments).slice(comments, 2)) shouldBe List("a", "b")
    }
  }

  "the fake's sort" should {

    "refuse to rank by text score, which needs a real text index" in {
      val refused = intercept[UnsupportedOperationException] {
        collection.find().sort(Sort.byTextScore()).all.unsafeRunSync()
      }

      refused.getMessage should include("textScore")
    }
  }
