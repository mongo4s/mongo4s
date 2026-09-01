package mongo4s.testkit

import scala.collection.mutable

import org.bson.{BsonDocument, BsonString, BsonValue}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.{Effect, Field, MongoCollection, Streamable}
import mongo4s.bson.{BsonDocumentCodec, DecodeResult, FieldNaming}
import mongo4s.queries.{AggregateQuery, DecodeAttempts, DistinctQuery, FindQuery}
import mongo4s.operations.{Filter, Index, Projection, Sort, Stage, Update, WriteCommand}
import mongo4s.results.{BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult}

import scala.jdk.CollectionConverters.given

final class FakeMongoCollection[F[*], S[*], E](
    val codec: BsonDocumentCodec[E],
    emit: List[E] => S[E],
    emitAttempts: List[DecodeResult[E]] => S[DecodeResult[E]] = (_: List[DecodeResult[E]]) =>
      throw UnsupportedOperationException("FakeMongoCollection: pass emitAttempts to stream decode attempts"),
    val name: String = "fake",
    val naming: FieldNaming = FieldNaming.identity,
)(using F: Effect[F])
    extends MongoCollection[F, S, E]:

  private val storage        = mutable.ArrayBuffer.empty[BsonDocument]
  private val createdIndexes = mutable.ArrayBuffer.empty[Index[E]]

  def snapshot: List[E] = storage.flatMap(codec.decodeDocument(_).toOption).toList

  def indexes: List[Index[E]] = createdIndexes.toList

  def insertRaw(document: BsonDocument): Unit = storage += document

  def insertOne(document: E)(using session: Option[ClientSession]): F[InsertOneResult] = F.delay {
    val encoded = codec.encodeDocument(document)
    storage += encoded
    InsertOneResult(Option(encoded.get("_id")))
  }

  def insertMany(documents: Seq[E])(using session: Option[ClientSession]): F[InsertManyResult] = F.delay {
    val encoded = documents.map(codec.encodeDocument)
    storage ++= encoded
    InsertManyResult(encoded.flatMap(d => Option(d.get("_id"))).toList)
  }

  def find(filter: Filter[E])(using session: Option[ClientSession]): FindQuery[F, S, E] = FakeFindQuery(filter)

  def replaceOne(filter: Filter[E], replacement: E, upsert: Boolean)(using session: Option[ClientSession]): F[UpdateResult] = F.delay {
    matching(filter).headOption match
      case Some(existing) =>
        storage(storage.indexOf(existing)) = codec.encodeDocument(replacement)
        UpdateResult(matchedCount = 1, modifiedCount = 1, upsertedId = None)
      case None if upsert =>
        val encoded = codec.encodeDocument(replacement)
        storage += encoded
        UpdateResult(matchedCount = 0, modifiedCount = 0, upsertedId = Option(encoded.get("_id")))
      case None           => UpdateResult.none
  }

  def updateOne(filter: Filter[E], update: Update[E], upsert: Boolean)(using session: Option[ClientSession]): F[UpdateResult] = F.delay {
    matching(filter).headOption match
      case Some(existing) =>
        storage(storage.indexOf(existing)) = applyUpdate(existing, update)
        UpdateResult(matchedCount = 1, modifiedCount = 1, upsertedId = None)
      case None if upsert =>
        throw UnsupportedOperationException("FakeMongoCollection: updateOne(upsert = true) is not simulated")
      case None           => UpdateResult.none
  }

  def updateMany(filter: Filter[E], update: Update[E], upsert: Boolean)(using session: Option[ClientSession]): F[UpdateResult] = F.delay {
    val matches = matching(filter)
    matches.foreach(doc => storage(storage.indexOf(doc)) = applyUpdate(doc, update))
    if matches.isEmpty && upsert then throw UnsupportedOperationException("FakeMongoCollection: updateMany(upsert = true) is not simulated")
    UpdateResult(matchedCount = matches.size.toLong, modifiedCount = matches.size.toLong, upsertedId = None)
  }

  def findOneAndUpdate(filter: Filter[E], update: Update[E], returnUpdated: Boolean, upsert: Boolean, sort: Sort[E], projection: Projection[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] = F.delay {
    matching(filter).headOption match
      case None if upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndUpdate(upsert = true) is not simulated")
      case None           => None
      case Some(existing) =>
        val updated = applyUpdate(existing, update)
        storage(storage.indexOf(existing)) = updated
        decodeProjected(if returnUpdated then updated else existing, projection)
  }

  def findOneAndReplace(filter: Filter[E], replacement: E, returnUpdated: Boolean, upsert: Boolean, sort: Sort[E], projection: Projection[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] = F.delay {
    matching(filter).headOption match
      case None if upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndReplace(upsert = true) is not simulated")
      case None           => None
      case Some(existing) =>
        val encoded = codec.encodeDocument(replacement)
        storage(storage.indexOf(existing)) = encoded
        decodeProjected(if returnUpdated then encoded else existing, projection)
  }

  def findOneAndDelete(filter: Filter[E], sort: Sort[E], projection: Projection[E])(using session: Option[ClientSession]): F[Option[E]] = F.delay {
    matching(filter).headOption.flatMap { existing =>
      storage -= existing
      decodeProjected(existing, projection)
    }
  }

  def deleteOne(filter: Filter[E])(using session: Option[ClientSession]): F[DeleteResult] = F.delay {
    matching(filter).headOption match
      case Some(existing) => storage -= existing; DeleteResult(1)
      case None           => DeleteResult.none
  }

  def deleteMany(filter: Filter[E])(using session: Option[ClientSession]): F[DeleteResult] = F.delay {
    val matches = matching(filter)
    storage --= matches
    DeleteResult(matches.size.toLong)
  }

  def count(filter: Filter[E])(using session: Option[ClientSession]): F[Long] = F.delay(matching(filter).size.toLong)

  def estimatedCount: F[Long] = F.delay(storage.size.toLong)

  def bulkWrite(commands: Seq[WriteCommand[E]], ordered: Boolean)(using session: Option[ClientSession]): F[BulkWriteResult] = F.delay {
    var inserted = 0L
    var matched  = 0L
    var modified = 0L
    var deleted  = 0L

    val upserted = mutable.Map.empty[Int, BsonValue]

    commands.zipWithIndex.foreach {
      case (WriteCommand.InsertOne(document), _)           =>
        storage += codec.encodeDocument(document)
        inserted += 1
      case (WriteCommand.ReplaceOne(filter, value, up), i) =>
        matching(filter).headOption match
          case Some(existing) =>
            storage(storage.indexOf(existing)) = codec.encodeDocument(value)
            matched += 1
            modified += 1
          case None if up     =>
            val encoded = codec.encodeDocument(value)
            storage += encoded
            upserted += (i -> upsertedId(encoded))
          case None           => ()
      case (WriteCommand.UpdateOne(filter, update, _), _)  =>
        matching(filter).headOption.foreach { doc =>
          storage(storage.indexOf(doc)) = applyUpdate(doc, update)
          matched += 1
          modified += 1
        }
      case (WriteCommand.UpdateMany(filter, update, _), _) =>
        matching(filter).foreach { doc =>
          storage(storage.indexOf(doc)) = applyUpdate(doc, update)
          matched += 1
          modified += 1
        }
      case (WriteCommand.DeleteOne(filter), _)             =>
        matching(filter).headOption.foreach { doc => storage -= doc; deleted += 1 }
      case (WriteCommand.DeleteMany(filter), _)            =>
        val matches = matching(filter)
        storage --= matches
        deleted += matches.size
    }

    BulkWriteResult(inserted, matched, modified, deleted, upserted.toMap)
  }

  def aggregate[B](pipeline: Seq[Stage[E]])(using session: Option[ClientSession])(using BsonDocumentCodec[B]): AggregateQuery[F, S, B] =
    throw UnsupportedOperationException("FakeMongoCollection: aggregate is not simulated")

  def distinct[B](field: Field[E, B], filter: Filter[E])(using
      session: Option[ClientSession]
  )(using
      mongo4s.bson.BsonDecoder[B]
  ): DistinctQuery[F, S, B] =
    throw UnsupportedOperationException("FakeMongoCollection: distinct is not simulated")

  def createIndex(index: Index[E])(using session: Option[ClientSession]): F[String] = F.delay {
    createdIndexes += index
    index.name.getOrElse(index.keysToBson(naming).keySet.asScala.mkString("_"))
  }

  def listIndexes(using session: Option[ClientSession]): F[List[BsonDocument]] =
    F.delay(createdIndexes.map(_.keysToBson(naming)).toList)

  def dropIndex(indexName: String)(using session: Option[ClientSession]): F[Unit] =
    F.delay(createdIndexes.filterInPlace(_.name.forall(_ != indexName)))

  def drop(using session: Option[ClientSession]): F[Unit] = F.delay { storage.clear(); createdIndexes.clear() }

  def watchAttempting(options: WatchOptions[E])(using
      session: Option[ClientSession]
  )(using Streamable[S, DecodeResult[ChangeEvent[E]]]): S[DecodeResult[ChangeEvent[E]]] =
    throw UnsupportedOperationException("FakeMongoCollection: watch is not simulated")

  def watch(options: WatchOptions[E])(using session: Option[ClientSession])(using Streamable[S, ChangeEvent[E]]): S[ChangeEvent[E]] =
    throw UnsupportedOperationException("FakeMongoCollection: watch is not simulated")

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

    case Filter.All(path, values) => elementsAt(document, path).exists(elements => values.forall(elements.contains))
    case Filter.Size(path, size)  => at(document, path).exists(v => v.isArray && v.asArray.size == size)
    case Filter.Type(path, name)  => at(document, path).exists(v => mongo4s.bson.BsonTypeName.of(v) == name)

    case Filter.Mod(path, divisor, remainder) =>
      at(document, path).exists(v => v.isNumber && v.asNumber.longValue % divisor == remainder)

    case Filter.ElemMatch(path, inner) =>
      elementsAt(document, path).exists(_.exists(element => element.isDocument && matches(element.asDocument, inner.asInstanceOf[Filter[E]])))

    case Filter.Text(_, _) =>
      throw UnsupportedOperationException("FakeMongoCollection: $text needs a real text index, so it is not simulated")

    case Filter.Expr(_) =>
      throw UnsupportedOperationException("FakeMongoCollection: $expr is not simulated")

    case Filter.Raw(_) => throw UnsupportedOperationException("FakeMongoCollection: Filter.Raw is not simulated")

  private def storedSegments(path: mongo4s.FieldPath): List[String] = path.render(naming).split('.').toList

  private def elementsAt(document: BsonDocument, path: mongo4s.FieldPath): Option[List[BsonValue]] =
    at(document, path).collect { case array if array.isArray => array.asArray.getValues.asScala.toList }

  private def at(document: BsonDocument, path: mongo4s.FieldPath): Option[BsonValue] =
    def go(current: BsonValue, segments: List[String]): Option[BsonValue] = segments match
      case Nil         => Some(current)
      case seg :: rest => if current.isDocument then Option(current.asDocument.get(seg)).flatMap(go(_, rest)) else None
    go(document, storedSegments(path))
  end at

  private def applyUpdate(document: BsonDocument, update: Update[E]): BsonDocument = update match
    case Update.Set(path, value)  => setAt(document, storedSegments(path), value)
    case Update.Unset(path)       => unsetAt(document, storedSegments(path))
    case Update.Inc(path, amount) =>
      val current = at(document, path).map(_.asNumber.longValue).getOrElse(0L)
      setAt(document, storedSegments(path), org.bson.BsonInt64(current + amount.asNumber.longValue))
    case Update.Combine(updates)  => updates.foldLeft(document)(applyUpdate)
    case other                    => throw UnsupportedOperationException(s"FakeMongoCollection: $other is not simulated")

  private def upsertedId(document: BsonDocument): BsonValue =
    Option(document.get("_id"))
      .orElse(Option(document.get("id")))
      .getOrElse(BsonString(document.toJson))

  private def decodeProjected(document: BsonDocument, projection: Projection[E]): Option[E] =
    codec.decodeDocument(applyProjection(document, projection)).toOption

  private def applyProjection(document: BsonDocument, projection: Projection[E]): BsonDocument =
    projection match
      case Projection.Everything() => document

      case Projection.Exclude(fields) =>
        fields.foldLeft(document)((acc, path) => unsetAt(acc, storedSegments(path)))

      case Projection.Include(fields, withId) =>
        val kept = fields.foldLeft(BsonDocument()) { (acc, path) =>
          at(document, path).fold(acc)(value => setAt(acc, storedSegments(path), value))
        }

        if withId
        then Option(document.get("_id")).fold(kept)(id => kept.append("_id", id))
        else kept
  end applyProjection

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

  private final class FakeFindQuery(
      filter: Filter[E],
      projection: Projection[E] = Projection.empty[E],
      ordering: Sort[E] = Sort.empty[E],
      skipped: Option[Int] = None,
      limited: Option[Int] = None,
  ) extends FindQuery[F, S, E]:

    def filter(other: Filter[E]): FindQuery[F, S, E]     = FakeFindQuery(Filter.and(filter, other), projection, ordering, skipped, limited)
    def sort(value: Sort[E]): FindQuery[F, S, E]         = FakeFindQuery(filter, projection, value, skipped, limited)
    def projection(p: Projection[E]): FindQuery[F, S, E] = FakeFindQuery(filter, p, ordering, skipped, limited)
    def skip(n: Int): FindQuery[F, S, E]                 = FakeFindQuery(filter, projection, ordering, Some(n), limited)
    def limit(n: Int): FindQuery[F, S, E]                = FakeFindQuery(filter, projection, ordering, skipped, Some(n))

    def hint(keys: BsonDocument): FindQuery[F, S, E]                                    = this
    def collation(value: com.mongodb.client.model.Collation): FindQuery[F, S, E]        = this
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): FindQuery[F, S, E] = this
    def batchSize(n: Int): FindQuery[F, S, E]                                           = this
    def comment(value: String): FindQuery[F, S, E]                                      = this

    def first: F[Option[E]]                  = F.delay(results.headOption)
    def all: F[List[E]]                      = F.delay(results)
    def stream(using Streamable[S, E]): S[E] = emit(results)

    def attempting: DecodeAttempts[F, S, E] = new DecodeAttempts[F, S, E]:
      def all: F[List[DecodeResult[E]]] = F.delay(documents.map(codec.decodeDocument))

      def stream(using Streamable[S, DecodeResult[E]]): S[DecodeResult[E]] = emitAttempts(documents.map(codec.decodeDocument))

    private def documents: List[BsonDocument] =
      val ordered =
        if ordering.isEmpty
        then matching(filter)
        else matching(filter).sortWith(before)

      val afterSkip = skipped.fold(ordered)(ordered.drop)
      limited.fold(afterSkip)(afterSkip.take).map(applyProjection(_, projection))
    end documents

    private def results: List[E] =
      documents.map { document =>
        codec
          .decodeDocument(document)
          .fold(error => throw error.toThrowable, identity)
      }

    private def before(left: BsonDocument, right: BsonDocument): Boolean =
      ordering.fields.view.map { (path, ascending) =>
        val comparison = (at(left, path), at(right, path)) match
          case (Some(l), Some(r)) => BsonOrdering.compare(l, r)
          case (None, Some(_))    => -1
          case (Some(_), None)    => 1
          case (None, None)       => 0
        if ascending then comparison else -comparison
      }.find(_ != 0).exists(_ < 0)

private object BsonOrdering:
  def compare(a: BsonValue, b: BsonValue): Int =
    if a.isNumber && b.isNumber then java.lang.Double.compare(a.asNumber.doubleValue, b.asNumber.doubleValue)
    else if a.isString && b.isString then a.asString.getValue.compareTo(b.asString.getValue)
    else throw UnsupportedOperationException(s"FakeMongoCollection: cannot compare $a and $b")
