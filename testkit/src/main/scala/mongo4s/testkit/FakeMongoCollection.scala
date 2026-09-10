package mongo4s.testkit

import scala.collection.mutable

import org.bson.{BsonArray, BsonDocument, BsonDouble, BsonInt32, BsonInt64, BsonNull, BsonObjectId, BsonString, BsonValue}
import com.mongodb.{ExplainVerbosity, MongoWriteException, ReadConcern, ReadPreference, ServerAddress, WriteConcern, WriteError}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.operations.*
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.{Effect, Field, MongoCollection, MongoError, Streamable}
import mongo4s.bson.{BsonDocumentCodec, BsonDocumentDecoder, DecodeResult, FieldNaming}
import mongo4s.queries.{AggregateQuery, DecodeAttempts, DistinctQuery, FindQuery, SelectQuery}
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

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, E]          = this
  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, E]        = this
  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, E] = this

  def snapshot: List[E] = storage.flatMap(codec.decodeDocument(_).toOption).toList

  def indexes: List[Index[E]] = createdIndexes.toList

  def insertRaw(document: BsonDocument): Unit = storage += document

  private def requireDefaultUpdateOptions(options: UpdateOptions, operation: String): Unit =
    requireNoArrayFilters(options.arrayFilters, operation)

    if options.upsert
    then throw UnsupportedOperationException(s"FakeMongoCollection: $operation with upsert is not simulated")

  private def requireNoArrayFilters(arrayFilters: Seq[Filter[?]], operation: String): Unit =
    if arrayFilters.nonEmpty
    then throw UnsupportedOperationException(s"FakeMongoCollection: $operation with arrayFilters is not simulated")

  def insertOne(document: E)(using session: Option[ClientSession]): F[InsertOneResult] =
    F.delay {
      val encoded = identified(codec.encodeDocument(document))
      storage += encoded
      InsertOneResult(Option(encoded.get("_id")))
    }

  def insertMany(documents: Seq[E])(using session: Option[ClientSession]): F[InsertManyResult] =
    F.delay {
      val encoded = documents.map(document => identified(codec.encodeDocument(document)))
      storage ++= encoded
      InsertManyResult(encoded.flatMap(d => Option(d.get("_id"))).toList)
    }

  private def identified(document: BsonDocument): BsonDocument =
    val stamped =
      if document.containsKey("_id")
      then document
      else copyOf(document).append("_id", BsonObjectId(org.bson.types.ObjectId.get()))

    val id = stamped.get("_id")

    if storage.exists(existing => Option(existing.get("_id")).contains(id))
    then
      throw MongoError.DuplicateKey(
        MongoWriteException(WriteError(11000, s"E11000 duplicate key error: _id $id", BsonDocument()), ServerAddress(), java.util.Collections.emptyList())
      )
    else stamped
  end identified

  def find(filter: Filter[E])(using session: Option[ClientSession]): FindQuery[F, S, E] = FakeFindQuery(filter)

  def replaceOne(filter: Filter[E], replacement: E, options: ReplaceOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.delay {
      matching(filter).headOption match
        case Some(existing)         =>
          storage(storage.indexOf(existing)) = codec.encodeDocument(replacement)
          UpdateResult(matchedCount = 1, modifiedCount = 1, upsertedId = None)
        case None if options.upsert =>
          val encoded = codec.encodeDocument(replacement)
          storage += encoded
          UpdateResult(matchedCount = 0, modifiedCount = 0, upsertedId = Option(encoded.get("_id")))
        case None                   => UpdateResult.none
    }

  def updateOne(filter: Filter[E], update: Update[E], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.delay {
      requireNoArrayFilters(options.arrayFilters, "updateOne")

      matching(filter).headOption match
        case Some(existing)         =>
          storage(storage.indexOf(existing)) = applyUpdate(existing, update)
          UpdateResult(matchedCount = 1, modifiedCount = 1, upsertedId = None)
        case None if options.upsert =>
          throw UnsupportedOperationException("FakeMongoCollection: updateOne with upsert is not simulated")
        case None                   => UpdateResult.none
    }

  def updateMany(filter: Filter[E], update: Update[E], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.delay {
      requireNoArrayFilters(options.arrayFilters, "updateMany")

      val matches = matching(filter)
      matches.foreach(doc => storage(storage.indexOf(doc)) = applyUpdate(doc, update))
      if matches.isEmpty && options.upsert then throw UnsupportedOperationException("FakeMongoCollection: updateMany with upsert is not simulated")
      UpdateResult(matchedCount = matches.size.toLong, modifiedCount = matches.size.toLong, upsertedId = None)
    }

  def findOneAndUpdate(filter: Filter[E], update: Update[E], options: FindOneAndUpdateOptions[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] =
    F.delay {
      requireNoArrayFilters(options.arrayFilters, "findOneAndUpdate")

      firstMatching(filter, options.sort) match
        case None if options.upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndUpdate with upsert is not simulated")
        case None                   => None
        case Some(existing)         =>
          val updated = applyUpdate(existing, update)
          storage(storage.indexOf(existing)) = updated
          decodeProjected(if options.returnUpdated then updated else existing, options.projection)
    }

  def findOneAndReplace(filter: Filter[E], replacement: E, options: FindOneAndReplaceOptions[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] =
    F.delay {
      firstMatching(filter, options.sort) match
        case None if options.upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndReplace with upsert is not simulated")
        case None                   => None
        case Some(existing)         =>
          val encoded = codec.encodeDocument(replacement)
          storage(storage.indexOf(existing)) = encoded
          decodeProjected(if options.returnUpdated then encoded else existing, options.projection)
    }

  def findOneAndDelete(filter: Filter[E], options: FindOneAndDeleteOptions[E])(using session: Option[ClientSession]): F[Option[E]] =
    F.delay {
      firstMatching(filter, options.sort).flatMap { existing =>
        storage -= existing
        decodeProjected(existing, options.projection)
      }
    }

  def deleteOne(filter: Filter[E], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    F.delay {
      matching(filter).headOption match
        case Some(existing) => storage -= existing; DeleteResult(1)
        case None           => DeleteResult.none
    }

  def deleteMany(filter: Filter[E], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    F.delay {
      val matches = matching(filter)
      storage --= matches
      DeleteResult(matches.size.toLong)
    }

  def count(filter: Filter[E], options: CountOptions)(using session: Option[ClientSession]): F[Long] =
    F.delay {
      val matches = matching(filter).size.toLong
      val skipped = options.skip.fold(matches)(skip => math.max(0L, matches - skip))
      options.limit.fold(skipped)(limit => math.min(skipped, limit.toLong))
    }

  def estimatedCount: F[Long] = F.delay(storage.size.toLong)

  def bulkWrite(commands: Seq[WriteCommand[E]], ordered: Boolean)(using session: Option[ClientSession]): F[BulkWriteResult] =
    F.delay {
      var inserted = 0L
      var matched  = 0L
      var modified = 0L
      var deleted  = 0L

      val upserted = mutable.Map.empty[Int, BsonValue]

      commands.zipWithIndex.foreach {
        case (WriteCommand.InsertOne(document), _)                 =>
          storage += codec.encodeDocument(document)
          inserted += 1
        case (WriteCommand.ReplaceOne(filter, value, options), i)  =>
          matching(filter).headOption match
            case Some(existing)         =>
              storage(storage.indexOf(existing)) = codec.encodeDocument(value)
              matched += 1
              modified += 1
            case None if options.upsert =>
              val encoded = codec.encodeDocument(value)
              storage += encoded
              upserted.update(i, upsertedId(encoded))
            case None                   => ()
        case (WriteCommand.UpdateOne(filter, update, options), _)  =>
          requireDefaultUpdateOptions(options, "bulkWrite UpdateOne")
          matching(filter).headOption.foreach { doc =>
            storage(storage.indexOf(doc)) = applyUpdate(doc, update)
            matched += 1
            modified += 1
          }
        case (WriteCommand.UpdateMany(filter, update, options), _) =>
          requireDefaultUpdateOptions(options, "bulkWrite UpdateMany")
          matching(filter).foreach { doc =>
            storage(storage.indexOf(doc)) = applyUpdate(doc, update)
            matched += 1
            modified += 1
          }
        case (WriteCommand.DeleteOne(filter), _)                   =>
          matching(filter).headOption.foreach { doc => storage -= doc; deleted += 1 }
        case (WriteCommand.DeleteMany(filter), _)                  =>
          val matches = matching(filter)
          storage --= matches
          deleted += matches.size
      }

      BulkWriteResult(inserted, matched, modified, deleted, upserted.toMap)
    }

  def aggregate[B](pipeline: Seq[Stage[E]])(using session: Option[ClientSession])(using decoderB: BsonDocumentDecoder[B]): AggregateQuery[F, S, B] =
    FakeAggregateQuery(pipeline.toList, decoderB)

  def distinct[B](field: Field[E, B], filter: Filter[E])(using
      session: Option[ClientSession]
  )(using
      decoder: mongo4s.bson.BsonDecoder[B]
  ): DistinctQuery[F, S, B] =
    FakeDistinctQuery(field.path, filter, decoder)

  def createIndex(index: Index[E])(using session: Option[ClientSession]): F[String] =
    F.delay {
      createdIndexes += index
      index.name.getOrElse(index.keysToBson(naming).keySet.asScala.mkString("_"))
    }

  def listIndexes(using session: Option[ClientSession]): F[List[BsonDocument]] =
    F.delay {
      createdIndexes.map(_.keysToBson(naming)).toList
    }

  def dropIndex(indexName: String)(using session: Option[ClientSession]): F[Unit] =
    F.delay {
      createdIndexes.filterInPlace(_.name.forall(_ != indexName))
    }

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

  private def firstMatching(filter: Filter[E], sort: Sort[E]): Option[BsonDocument] =
    val matched = matching(filter)
    (if sort.isEmpty then matched else matched.sortWith(orderedBy(sort))).headOption

  private def matches(document: BsonDocument, filter: Filter[E]): Boolean = filter match
    case Filter.Eq(path, value)               => equals(document, path, value)
    case Filter.Ne(path, value)               => !equals(document, path, value)
    case Filter.Gt(path, value)               => leaves(document, path).exists(BsonOrdering.compare(_, value) > 0)
    case Filter.Gte(path, value)              => leaves(document, path).exists(BsonOrdering.compare(_, value) >= 0)
    case Filter.Lt(path, value)               => leaves(document, path).exists(BsonOrdering.compare(_, value) < 0)
    case Filter.Lte(path, value)              => leaves(document, path).exists(BsonOrdering.compare(_, value) <= 0)
    case Filter.In(path, values)              => values.exists(equals(document, path, _))
    case Filter.Nin(path, values)             => !values.exists(equals(document, path, _))
    case Filter.Exists(path, exists)          => candidates(document, path).nonEmpty == exists
    case Filter.Regex(path, pattern, options) =>
      val compiled = BsonOrdering.regex(pattern, options)
      leaves(document, path).exists(value => value.isString && compiled.matcher(value.asString.getValue).find())
    case Filter.And(filters)                  => filters.forall(matches(document, _))
    case Filter.Or(filters)                   => filters.exists(matches(document, _))
    case Filter.Not(inner)                    => !matches(document, inner)
    case Filter.MatchAll()                    => true
    case Filter.MatchNone()                   => false

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

    case near @ (Filter.Near(_, _, _, _) | Filter.NearSphere(_, _, _, _)) =>
      throw UnsupportedOperationException(
        s"FakeMongoCollection: ${near.key} needs a geospatial index and a real distance, so it is not simulated"
      )

    case Filter.GeoWithin(_, _) =>
      throw UnsupportedOperationException("FakeMongoCollection: $geoWithin is not simulated")

    case Filter.GeoIntersects(_, _) =>
      throw UnsupportedOperationException("FakeMongoCollection: $geoIntersects is not simulated")

    case Filter.Raw(_) => throw UnsupportedOperationException("FakeMongoCollection: Filter.Raw is not simulated")

  private def storedSegments(path: mongo4s.FieldPath): List[String] = path.render(naming).split('.').toList

  private def elementsAt(document: BsonDocument, path: mongo4s.FieldPath): Option[List[BsonValue]] =
    at(document, path).collect { case array if array.isArray => array.asArray.getValues.asScala.toList }

  private def candidates(document: BsonDocument, path: mongo4s.FieldPath): List[BsonValue] =
    def go(current: BsonValue, segments: List[String]): List[BsonValue] = segments match
      case Nil         => List(current)
      case seg :: rest =>
        current match
          case nested: BsonDocument => Option(nested.get(seg)).toList.flatMap(go(_, rest))
          case array: BsonArray     => array.getValues.asScala.toList.flatMap(go(_, segments))
          case _                    => Nil

    go(document, storedSegments(path))
  end candidates

  private def leaves(document: BsonDocument, path: mongo4s.FieldPath): List[BsonValue] =
    candidates(document, path).flatMap {
      case array: BsonArray => array :: array.getValues.asScala.toList
      case value            => List(value)
    }

  private def equals(document: BsonDocument, path: mongo4s.FieldPath, value: BsonValue): Boolean =
    if value.isNull
    then candidates(document, path).isEmpty || leaves(document, path).exists(_.isNull)
    else leaves(document, path).contains(value)

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
      setAt(document, storedSegments(path), BsonOrdering.increment(at(document, path), amount))
    case Update.Combine(updates)  => updates.foldLeft(document)(applyUpdate)
    case other                    => throw UnsupportedOperationException(s"FakeMongoCollection: $other is not simulated")

  private def upsertedId(document: BsonDocument): BsonValue =
    Option(document.get("_id"))
      .orElse(Option(document.get("id")))
      .getOrElse(BsonString(document.toJson))

  private def decodeProjected(document: BsonDocument, projection: Projection[E]): Option[E] =
    codec.decodeDocument(applyProjection(document, projection)).toOption

  private def applyProjection(document: BsonDocument, projection: Projection[E]): BsonDocument =
    val projected = projection match
      case Projection.Everything(_) => document

      case Projection.Exclude(fields, _) =>
        fields.foldLeft(document)((acc, path) => unsetAt(acc, storedSegments(path)))

      case Projection.Include(fields, withId, slices) =>
        val kept = (fields ++ slices.map(_._1)).foldLeft(BsonDocument()) { (acc, path) =>
          at(document, path).fold(acc)(value => setAt(acc, storedSegments(path), value))
        }

        if withId
        then Option(document.get("_id")).fold(kept)(id => kept.append("_id", id))
        else kept

    projection.slices.foldLeft(projected) { (acc, entry) =>
      at(acc, entry._1) match
        case Some(array) if array.isArray =>
          setAt(acc, storedSegments(entry._1), sliced(array.asArray, entry._2))
        case _                            => acc
    }
  end applyProjection

  private def sliced(array: BsonArray, slice: Slice): BsonArray =
    val values = array.getValues.asScala.toList

    val taken = slice.skip match
      case Some(from)              => values.slice(from, from + slice.count)
      case None if slice.count < 0 => values.takeRight(-slice.count)
      case None                    => values.take(slice.count)

    BsonArray(taken.asJava)
  end sliced

  private def copyOf(document: BsonDocument): BsonDocument =
    document.entrySet.asScala.foldLeft(BsonDocument()) { (acc, e) =>
      acc.append(e.getKey, e.getValue)
    }

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

  private def orderedBy(sort: Sort[E]): (BsonDocument, BsonDocument) => Boolean =
    if sort.fields.exists(_._2 == SortOrder.TextScore)
    then
      throw UnsupportedOperationException(
        "FakeMongoCollection: sorting by $meta textScore needs a real text index, so it is not simulated"
      )

    (left, right) =>
      sort.fields.view.map { (path, order) =>
        val comparison = (at(left, path), at(right, path)) match
          case (Some(l), Some(r)) => BsonOrdering.compare(l, r)
          case (None, Some(_))    => -1
          case (Some(_), None)    => 1
          case (None, None)       => 0

        if order == SortOrder.Ascending then comparison else -comparison
      }.find(_ != 0).exists(_ < 0)
  end orderedBy

  private def runPipeline(stages: List[Stage[E]]): List[BsonDocument] =
    stages.foldLeft(storage.toList)(runStage)

  private def runStage(documents: List[BsonDocument], stage: Stage[E]): List[BsonDocument] = stage match
    case Stage.MatchStage(filter)      => documents.filter(matches(_, filter))
    case Stage.SortStage(sort)         => documents.sortWith(orderedBy(sort))
    case Stage.Skip(n)                 => documents.drop(n)
    case Stage.Limit(n)                => documents.take(n)
    case Stage.ProjectStage(selected)  => documents.map(applyProjection(_, selected))
    case Stage.Count(fieldName)        =>
      if documents.isEmpty then Nil else List(BsonDocument(fieldName, BsonInt32(documents.size)))
    case Stage.Group(by, accumulators) => grouped(documents, by, accumulators)
    case other                         =>
      throw UnsupportedOperationException(s"FakeMongoCollection: ${stageName(other)} is not simulated")

  private def stageName(stage: Stage[E]): String =
    if stage.key.nonEmpty
    then stage.key
    else
      stage match
        case Stage.Raw(document) if !document.isEmpty => s"the raw stage ${document.getFirstKey}"
        case _                                        => "a raw stage"

  private def grouped(
      documents: List[BsonDocument],
      by: Option[mongo4s.FieldPath],
      accumulators: List[(String, Accumulator[E])],
  ): List[BsonDocument] =
    val keyed = documents.map(document => keyOf(document, by) -> document)

    keyed.map(_._1).distinct.map { key =>
      val members = keyed.collect { case (candidate, document) if candidate == key => document }
      accumulators.foldLeft(BsonDocument("_id", key)) { (acc, entry) =>
        acc.append(entry._1, accumulated(entry._2, members))
      }
    }
  end grouped

  private def keyOf(document: BsonDocument, by: Option[mongo4s.FieldPath]): BsonValue =
    by.flatMap(at(document, _)).getOrElse(BsonNull.VALUE)

  private def accumulated(accumulator: Accumulator[E], documents: List[BsonDocument]): BsonValue = accumulator match
    case Accumulator.Count()           => BsonInt32(documents.size)
    case Accumulator.Sum(expression)   => summed(valuesOf(expression, documents))
    case Accumulator.Avg(expression)   => averaged(valuesOf(expression, documents))
    case Accumulator.Min(expression)   => extreme(valuesOf(expression, documents), _ < 0)
    case Accumulator.Max(expression)   => extreme(valuesOf(expression, documents), _ > 0)
    case Accumulator.First(expression) => positional(expression, documents.headOption)
    case Accumulator.Last(expression)  => positional(expression, documents.lastOption)
    case Accumulator.Push(expression)  => BsonArray(valuesOf(expression, documents).asJava)

    case Accumulator.AddToSet(_) =>
      throw UnsupportedOperationException(
        "FakeMongoCollection: $addToSet is not simulated because MongoDB leaves the order of its result undefined"
      )

    case Accumulator.Raw(document) =>
      throw UnsupportedOperationException(s"FakeMongoCollection: the raw accumulator $document is not simulated")

  private def positional(expression: Accumulator.Expression[E], document: Option[BsonDocument]): BsonValue =
    document.flatMap(one => valuesOf(expression, List(one)).headOption).getOrElse(BsonNull.VALUE)

  private def valuesOf(expression: Accumulator.Expression[E], documents: List[BsonDocument]): List[BsonValue] =
    expression match
      case Accumulator.Expression.FieldRef(path) => documents.flatMap(at(_, path))
      case Accumulator.Expression.Literal(value) => documents.map(_ => value)

  private def summed(values: List[BsonValue]): BsonValue =
    if values.exists(_.isDecimal128)
    then throw UnsupportedOperationException("FakeMongoCollection: $sum over a Decimal128 is not simulated")
    else
      val numbers = values.filter(_.isNumber)

      if numbers.exists(_.isDouble)
      then BsonDouble(numbers.map(_.asNumber.doubleValue).sum)
      else
        val total = numbers.map(_.asNumber.longValue).sum
        if numbers.forall(_.isInt32) && total.isValidInt then BsonInt32(total.toInt) else BsonInt64(total)
  end summed

  private def averaged(values: List[BsonValue]): BsonValue =
    if values.exists(_.isDecimal128)
    then throw UnsupportedOperationException("FakeMongoCollection: $avg over a Decimal128 is not simulated")
    else
      val numbers = values.filter(_.isNumber)
      if numbers.isEmpty then BsonNull.VALUE else BsonDouble(numbers.map(_.asNumber.doubleValue).sum / numbers.size)

  private def extreme(values: List[BsonValue], keep: Int => Boolean): BsonValue =
    values.reduceOption((left, right) => if keep(BsonOrdering.compare(left, right)) then left else right).getOrElse(BsonNull.VALUE)

  private def distinctValues(path: mongo4s.FieldPath, filter: Filter[E]): List[BsonValue] =
    matching(filter).flatMap { document =>
      at(document, path) match
        case Some(array) if array.isArray => array.asArray.getValues.asScala.toList
        case other                        => other.toList
    }.distinct

  private final class FakeDistinctQuery[B](
      path: mongo4s.FieldPath,
      filter: Filter[E],
      decoder: mongo4s.bson.BsonDecoder[B],
  ) extends DistinctQuery[F, S, B]:

    def collation(value: com.mongodb.client.model.Collation): DistinctQuery[F, S, B]        = this
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): DistinctQuery[F, S, B] = this
    def batchSize(n: Int): DistinctQuery[F, S, B]                                           = this

    def first: F[Option[B]] = F.delay(decoded.headOption)
    def all: F[List[B]]     = F.delay(decoded)

    def stream(using Streamable[S, B]): S[B] =
      throw UnsupportedOperationException("FakeMongoCollection: streaming distinct values needs an emitter for their type")

    def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
      def all: F[List[DecodeResult[B]]] = F.delay(distinctValues(path, filter).map(decoder.decode))

      def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
        throw UnsupportedOperationException("FakeMongoCollection: streaming distinct values needs an emitter for their type")

    private def decoded: List[B] =
      distinctValues(path, filter).map(decoder.decode(_).fold(error => throw error.toThrowable, identity))

  private final class FakeAggregateQuery[B](stages: List[Stage[E]], decoderB: BsonDocumentDecoder[B]) extends AggregateQuery[F, S, B]:
    def allowDiskUse(allow: Boolean): AggregateQuery[F, S, B] = this

    def hint(keys: BsonDocument): AggregateQuery[F, S, B]                                    = this
    def collation(value: com.mongodb.client.model.Collation): AggregateQuery[F, S, B]        = this
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): AggregateQuery[F, S, B] = this
    def batchSize(n: Int): AggregateQuery[F, S, B]                                           = this
    def comment(value: String): AggregateQuery[F, S, B]                                      = this

    def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
      throw UnsupportedOperationException("FakeMongoCollection: explain needs a real query planner, so it is not simulated")

    def first: F[Option[B]] = F.delay(decoded.headOption)
    def all: F[List[B]]     = F.delay(decoded)

    def stream(using Streamable[S, B]): S[B] =
      throw UnsupportedOperationException("FakeMongoCollection: streaming an aggregation needs an emitter for its output type")

    def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
      def all: F[List[DecodeResult[B]]] = F.delay(runPipeline(stages).map(decoderB.decodeDocument))

      def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
        throw UnsupportedOperationException("FakeMongoCollection: streaming an aggregation needs an emitter for its output type")

    private def decoded: List[B] =
      runPipeline(stages).map(decoderB.decodeDocument(_).fold(error => throw error.toThrowable, identity))

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

    def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
      throw UnsupportedOperationException("FakeMongoCollection: explain needs a real query planner, so it is not simulated")

    def first: F[Option[E]]                  = F.delay(results.headOption)
    def all: F[List[E]]                      = F.delay(results)
    def stream(using Streamable[S, E]): S[E] = emit(results)

    def selecting[B](selected: Projection[E], decoder: FieldNaming => BsonDocumentDecoder[B]): SelectQuery[F, S, B] =
      val decode            = decoder(naming).decodeDocument
      val selectedDocuments = () => sourceDocuments.map(applyProjection(_, selected)).map(decode)

      new SelectQuery[F, S, B]:
        def first: F[Option[B]] = F.delay(selectedDocuments().headOption.map(orThrow))
        def all: F[List[B]]     = F.delay(selectedDocuments().map(orThrow))

        def stream(using Streamable[S, B]): S[B] =
          throw UnsupportedOperationException("FakeMongoCollection: streaming a selection needs an emitter for its element type")

        def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
          def all: F[List[DecodeResult[B]]] = F.delay(selectedDocuments())

          def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
            throw UnsupportedOperationException("FakeMongoCollection: streaming a selection needs an emitter for its element type")
    end selecting

    private def orThrow[B](result: DecodeResult[B]): B = result.fold(error => throw error.toThrowable, identity)

    def attempting: DecodeAttempts[F, S, E] = new DecodeAttempts[F, S, E]:
      def all: F[List[DecodeResult[E]]] = F.delay(documents.map(codec.decodeDocument))

      def stream(using Streamable[S, DecodeResult[E]]): S[DecodeResult[E]] = emitAttempts(documents.map(codec.decodeDocument))

    private def sourceDocuments: List[BsonDocument] =
      val ordered =
        if ordering.isEmpty
        then matching(filter)
        else matching(filter).sortWith(orderedBy(ordering))

      val afterSkip = skipped.fold(ordered)(ordered.drop)
      limited.fold(afterSkip)(afterSkip.take)
    end sourceDocuments

    private def documents: List[BsonDocument] = sourceDocuments.map(applyProjection(_, projection))

    private def results: List[E] =
      documents.map { document =>
        codec
          .decodeDocument(document)
          .fold(error => throw error.toThrowable, identity)
      }

private object BsonOrdering:
  def compare(a: BsonValue, b: BsonValue): Int =
    if a.isNumber && b.isNumber then java.lang.Double.compare(a.asNumber.doubleValue, b.asNumber.doubleValue)
    else if a.isString && b.isString then a.asString.getValue.compareTo(b.asString.getValue)
    else if a.isDateTime && b.isDateTime then java.lang.Long.compare(a.asDateTime.getValue, b.asDateTime.getValue)
    else if a.isObjectId && b.isObjectId then a.asObjectId.getValue.compareTo(b.asObjectId.getValue)
    else if a.isBoolean && b.isBoolean then java.lang.Boolean.compare(a.asBoolean.getValue, b.asBoolean.getValue)
    else if a.isTimestamp && b.isTimestamp then a.asTimestamp.getValue.compareTo(b.asTimestamp.getValue)
    else throw UnsupportedOperationException(s"FakeMongoCollection: cannot compare $a and $b")

  def regex(pattern: String, options: String): java.util.regex.Pattern =
    val flags = options.foldLeft(0) { (acc, flag) =>
      flag match
        case 'i' => acc | java.util.regex.Pattern.CASE_INSENSITIVE
        case 'm' => acc | java.util.regex.Pattern.MULTILINE
        case 's' => acc | java.util.regex.Pattern.DOTALL
        case 'x' => acc | java.util.regex.Pattern.COMMENTS
        case _   => acc
    }

    java.util.regex.Pattern.compile(pattern, flags)
  end regex

  def increment(current: Option[BsonValue], amount: BsonValue): BsonValue =
    val base = current.getOrElse(BsonInt32(0))

    if base.isDecimal128 || amount.isDecimal128
    then throw UnsupportedOperationException("FakeMongoCollection: $inc over a Decimal128 is not simulated")
    else if base.isDouble || amount.isDouble
    then BsonDouble(base.asNumber.doubleValue + amount.asNumber.doubleValue)
    else
      val total = base.asNumber.longValue + amount.asNumber.longValue
      if base.isInt32 && amount.isInt32 && total.isValidInt then BsonInt32(total.toInt) else BsonInt64(total)
  end increment
