package mongo4s.repositories

import scala.collection.mutable

import org.bson.{BsonDocument, BsonValue}
import com.mongodb.reactivestreams.client.MongoCollection as RSMongoCollection

import mongo4s.{Effect, Field, MongoCollection, Streamable}
import mongo4s.bson.{BsonDocumentCodec, FieldNaming}
import mongo4s.results.{InsertManyResult, InsertOneResult}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.operations.{Filter, Projection, Sort, Update, WriteCommand}

import scala.jdk.CollectionConverters.given

final class FakeMongoCollection[F[*], S[*], E](
    val codec: BsonDocumentCodec[E],
    emit: List[E] => S[E],
    val name: String = "fake",
    val naming: FieldNaming = FieldNaming.identity,
)(using F: Effect[F])
    extends MongoCollection[F, S, E]:

  private val storage = mutable.ArrayBuffer.empty[BsonDocument]

  def snapshot: List[E] = storage.flatMap(codec.decodeDocument(_).toOption).toList

  def insertOne(document: E): F[InsertOneResult] = F.delay {
    val encoded = codec.encodeDocument(document)
    storage += encoded
    InsertOneResult(Option(encoded.get("_id")))
  }

  def insertMany(documents: Seq[E]): F[InsertManyResult] = F.delay {
    val encoded = documents.map(codec.encodeDocument)
    storage ++= encoded
    InsertManyResult(encoded.flatMap(d => Option(d.get("_id"))).toList)
  }

  def find: FindQuery[F, S, E]                    = FakeFindQuery(Filter.all)
  def find(filter: Filter[E]): FindQuery[F, S, E] = FakeFindQuery(filter)

  def replaceOne(filter: Filter[E], replacement: E, upsert: Boolean): F[Long] = F.delay {
    matching(filter).headOption match
      case Some(existing) =>
        storage(storage.indexOf(existing)) = codec.encodeDocument(replacement)
        1L
      case None if upsert =>
        storage += codec.encodeDocument(replacement)
        1L
      case None           => 0L
  }

  def updateOne(filter: Filter[E], update: Update[E], upsert: Boolean): F[Long] = F.delay {
    matching(filter).headOption match
      case Some(existing) =>
        storage(storage.indexOf(existing)) = applyUpdate(existing, update)
        1L
      case None if upsert =>
        throw UnsupportedOperationException("FakeMongoCollection: updateOne(upsert = true) is not simulated")
      case None           => 0L
  }

  def updateMany(filter: Filter[E], update: Update[E], upsert: Boolean): F[Long] = F.delay {
    val matches = matching(filter)
    matches.foreach(doc => storage(storage.indexOf(doc)) = applyUpdate(doc, update))
    if matches.isEmpty && upsert then throw UnsupportedOperationException("FakeMongoCollection: updateMany(upsert = true) is not simulated")
    matches.size.toLong
  }

  def deleteOne(filter: Filter[E]): F[Long] = F.delay {
    matching(filter).headOption match
      case Some(existing) => storage -= existing; 1L
      case None           => 0L
  }

  def deleteMany(filter: Filter[E]): F[Long] = F.delay {
    val matches = matching(filter)
    storage --= matches
    matches.size.toLong
  }

  def count: F[Long]                    = F.delay(storage.size.toLong)
  def count(filter: Filter[E]): F[Long] = F.delay(matching(filter).size.toLong)

  def bulkWrite(commands: Seq[WriteCommand[E]]): F[Unit] = F.delay {
    commands.foreach {
      case WriteCommand.InsertOne(document)           => storage += codec.encodeDocument(document)
      case WriteCommand.ReplaceOne(filter, value, up) =>
        matching(filter).headOption match
          case Some(existing) => storage(storage.indexOf(existing)) = codec.encodeDocument(value)
          case None if up     => storage += codec.encodeDocument(value)
          case None           => ()
      case WriteCommand.UpdateOne(filter, update, _)  =>
        matching(filter).headOption.foreach(doc => storage(storage.indexOf(doc)) = applyUpdate(doc, update))
      case WriteCommand.UpdateMany(filter, update, _) =>
        matching(filter).foreach(doc => storage(storage.indexOf(doc)) = applyUpdate(doc, update))
      case WriteCommand.DeleteOne(filter)             => matching(filter).headOption.foreach(storage -= _)
      case WriteCommand.DeleteMany(filter)            => storage --= matching(filter)
    }
  }

  def aggregate[B](pipeline: Seq[BsonDocument])(using BsonDocumentCodec[B]): AggregateQuery[F, S, B] =
    throw UnsupportedOperationException("FakeMongoCollection: aggregate is not simulated")

  def distinct[C, B](field: Field[E, C], filter: Filter[E])(using mongo4s.bson.BsonDecoder[B]): DistinctQuery[F, S, B] =
    throw UnsupportedOperationException("FakeMongoCollection: distinct is not simulated")

  def watch(using Streamable[S, BsonDocument]): S[BsonDocument] = throw UnsupportedOperationException("FakeMongoCollection: watch is not simulated")

  def underlying: RSMongoCollection[BsonDocument] =
    throw UnsupportedOperationException("FakeMongoCollection: no real driver collection behind this fake")

  private def matching(filter: Filter[E]): List[BsonDocument] = storage.filter(matches(_, filter)).toList

  private def matches(document: BsonDocument, filter: Filter[E]): Boolean = filter match
    case Filter.Eq(path, value)         => at(document, path).contains(value)
    case Filter.Ne(path, value)         => !at(document, path).contains(value)
    case Filter.Gt(path, value)         => at(document, path).exists(BsonOrdering.compare(_, value) > 0)
    case Filter.Gte(path, value)        => at(document, path).exists(BsonOrdering.compare(_, value) >= 0)
    case Filter.Lt(path, value)         => at(document, path).exists(BsonOrdering.compare(_, value) < 0)
    case Filter.Lte(path, value)        => at(document, path).exists(BsonOrdering.compare(_, value) <= 0)
    case Filter.In(path, values)        => at(document, path).exists(values.contains)
    case Filter.Nin(path, values)       => !at(document, path).exists(values.contains)
    case Filter.Exists(path, exists)    => at(document, path).isDefined == exists
    case Filter.Regex(path, pattern, _) => at(document, path).exists(v => v.isString && v.asString.getValue.matches(pattern))
    case Filter.And(filters)            => filters.forall(matches(document, _))
    case Filter.Or(filters)             => filters.exists(matches(document, _))
    case Filter.Not(inner)              => !matches(document, inner)
    case Filter.MatchAll()              => true
    case Filter.MatchNone()             => false
    case Filter.Raw(_)                  => throw UnsupportedOperationException("FakeMongoCollection: Filter.Raw is not simulated")

  private def at(document: BsonDocument, path: mongo4s.FieldPath): Option[BsonValue] =
    def go(current: BsonValue, segments: List[String]): Option[BsonValue] = segments match
      case Nil         => Some(current)
      case seg :: rest => if current.isDocument then Option(current.asDocument.get(seg)).flatMap(go(_, rest)) else None
    go(document, path.segments)

  private def applyUpdate(document: BsonDocument, update: Update[E]): BsonDocument = update match
    case Update.Set(path, value)                                                       => setAt(document, path.segments, value)
    case Update.Unset(path)                                                            => unsetAt(document, path.segments)
    case Update.Inc(path, amount)                                                      =>
      val current = at(document, path).map(_.asNumber.longValue).getOrElse(0L)
      setAt(document, path.segments, org.bson.BsonInt64(current + amount.asNumber.longValue))
    case Update.Combine(updates)                                                       => updates.foldLeft(document)(applyUpdate)
    case Update.Push(_, _) | Update.Pull(_, _) | Update.AddToSet(_, _) | Update.Raw(_) =>
      throw UnsupportedOperationException(s"FakeMongoCollection: $update is not simulated")

  private def copyOf(document: BsonDocument): BsonDocument =
    document.entrySet.asScala.foldLeft(BsonDocument())((acc, e) => acc.append(e.getKey, e.getValue))

  private def setAt(document: BsonDocument, segments: List[String], value: BsonValue): BsonDocument =
    segments match
      case Nil         => document
      case seg :: Nil  => copyOf(document).append(seg, value)
      case seg :: rest =>
        val nested = Option(document.getDocument(seg, null)).getOrElse(BsonDocument())
        copyOf(document).append(seg, setAt(nested, rest, value))

  private def unsetAt(document: BsonDocument, segments: List[String]): BsonDocument =
    segments match
      case Nil         => document
      case seg :: Nil  => val copy = copyOf(document); copy.remove(seg); copy
      case seg :: rest =>
        Option(document.getDocument(seg, null)) match
          case None         => document
          case Some(nested) => copyOf(document).append(seg, unsetAt(nested, rest))

  private final class FakeFindQuery(filter: Filter[E], projection: Projection[E] = Projection.empty[E]) extends FindQuery[F, S, E]:
    def filter(other: Filter[E]): FindQuery[F, S, E]     = FakeFindQuery(Filter.and(filter, other), projection)
    def sort(sort: Sort[E]): FindQuery[F, S, E]          = this
    def projection(p: Projection[E]): FindQuery[F, S, E] = FakeFindQuery(filter, p)
    def skip(n: Int): FindQuery[F, S, E]                 = this
    def limit(n: Int): FindQuery[F, S, E]                = this

    def first: F[Option[E]]                  = F.delay(matching(filter).headOption.flatMap(codec.decodeDocument(_).toOption))
    def all: F[List[E]]                      = F.delay(matching(filter).flatMap(codec.decodeDocument(_).toOption))
    def stream(using Streamable[S, E]): S[E] = emit(matching(filter).flatMap(codec.decodeDocument(_).toOption))

private object BsonOrdering:
  def compare(a: BsonValue, b: BsonValue): Int =
    if a.isNumber && b.isNumber then java.lang.Double.compare(a.asNumber.doubleValue, b.asNumber.doubleValue)
    else if a.isString && b.isString then a.asString.getValue.compareTo(b.asString.getValue)
    else throw UnsupportedOperationException(s"FakeMongoCollection: cannot compare $a and $b")
