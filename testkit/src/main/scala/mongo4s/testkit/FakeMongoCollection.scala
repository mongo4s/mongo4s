package mongo4s.testkit

import java.util.concurrent.locks.ReentrantLock

import scala.annotation.unused
import scala.collection.mutable

import com.mongodb.bulk.BulkWriteError
import com.mongodb.MongoCommandException
import com.mongodb.client.model.Collation
import org.bson.{BsonArray, BsonBoolean, BsonDateTime, BsonDocument, BsonDouble, BsonInt32, BsonInt64, BsonNull, BsonObjectId, BsonString, BsonValue}
import com.mongodb.{ExplainVerbosity, MongoBulkWriteException, MongoWriteException, ReadConcern, ReadPreference, ServerAddress, WriteConcern, WriteError}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.operations.*
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.{DistinctOf, Effect, Field, FieldPath, MongoCollection, MongoError, RsBridge, Streamable}
import mongo4s.bson.{BsonDocumentCodec, BsonDocumentDecoder, BsonField, DecodeResult, FieldNaming}
import mongo4s.queries.{AggregateQuery, DecodeAttempts, DistinctQuery, FindQuery, SelectQuery}
import mongo4s.results.{BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult}

import scala.jdk.CollectionConverters.given

final class FakeMongoCollection[F[*], S[*], E](
    val codec: BsonDocumentCodec[E],
    val name: String = "fake",
    val naming: FieldNaming = FieldNaming.identity,
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoCollection[F, S, E]:

  private val storage        = mutable.ArrayBuffer.empty[BsonDocument]
  private val IdIndex        = "_id_"
  private val createdIndexes = mutable.ArrayBuffer.empty[Index[E]]
  private val lock           = ReentrantLock()

  private def locked[A](body: => A): A =
    lock.lock()
    try body
    finally lock.unlock()

  private def atomically[A](body: => A): F[A] = F.delay(locked(body))

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, E]          = this
  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, E]        = this
  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, E] = this

  def snapshot: List[E] = locked(storage.flatMap(codec.decodeDocument(_).toOption).toList)

  def indexes: List[Index[E]] = locked(createdIndexes.toList)

  def insertRaw(document: BsonDocument): Unit = locked(storage += document): Unit

  private def requireDefaultUpdateOptions(options: UpdateOptions, operation: String): Unit =
    requireNoArrayFilters(options.arrayFilters, operation)
    requireNoCollation(options.collation, operation)

    if options.upsert
    then throw UnsupportedOperationException(s"FakeMongoCollection: $operation with upsert is not simulated")

  private def requireNoArrayFilters(arrayFilters: Seq[Filter[?]], operation: String): Unit =
    if arrayFilters.nonEmpty
    then throw UnsupportedOperationException(s"FakeMongoCollection: $operation with arrayFilters is not simulated")

  private def requireNoCollation(collation: Option[Collation], operation: String): Unit =
    if collation.nonEmpty
    then throw UnsupportedOperationException(s"FakeMongoCollection: $operation with a collation is not simulated")

  private def requireRenderable(update: Update[E]): Unit =
    update.toPipeline(naming) match
      case Some(_) => throw UnsupportedOperationException("FakeMongoCollection: a pipeline update computes from the document on the server, so it is not simulated")
      case None    => update.toBson(naming): Unit

  private def refuseCollation: Nothing =
    throw UnsupportedOperationException("FakeMongoCollection: a collation changes what matches and how it sorts, so it is not simulated")

  private def modifiedBy(before: BsonDocument, after: BsonDocument): Long =
    if before == after then 0L else 1L

  private def replacing(existing: BsonDocument, replacement: BsonDocument): BsonDocument =
    (Option(existing.get(BsonField.Id)), Option(replacement.get(BsonField.Id))) match
      case (Some(id), None)                       =>
        val kept = BsonDocument(BsonField.Id, id)
        kept.putAll(replacement)
        kept
      case (Some(id), Some(other)) if id != other =>
        throw MongoError.translate(
          MongoWriteException(
            WriteError(66, s"After applying the update, the (immutable) field '_id' was found to have been altered to _id: $other", BsonDocument()),
            ServerAddress(),
            java.util.Collections.emptyList(),
          )
        )
      case _                                      => replacement
  end replacing

  def insertOne(document: E)(using session: Option[ClientSession]): F[InsertOneResult] =
    atomically {
      val encoded = identified(codec.encodeDocument(document))
      storage += encoded
      InsertOneResult(Option(encoded.get(BsonField.Id)))
    }

  def insertMany(documents: Seq[E])(using session: Option[ClientSession]): F[InsertManyResult] =
    atomically {
      val ids = mutable.ListBuffer.empty[BsonValue]

      documents.zipWithIndex.foreach { (document, i) =>
        val encoded =
          try identified(codec.encodeDocument(document))
          catch case duplicate: MongoError.DuplicateKey => throw bulkFailure(BulkWriteResult.none.copy(insertedCount = i.toLong), List(failureAt(i, duplicate)))

        storage += encoded
        ids ++= Option(encoded.get(BsonField.Id))
      }

      InsertManyResult(ids.toList)
    }

  private def failureAt(index: Int, error: MongoError): BulkWriteError =
    BulkWriteError(error.code, error.getMessage, BsonDocument(), index)

  private def bulkFailure(written: BulkWriteResult, failures: List[BulkWriteError]): MongoError =
    MongoError.bulkWriteFailed(MongoBulkWriteException(written.toDriver, failures.asJava, null, ServerAddress(), java.util.Set.of()))

  private def identified(document: BsonDocument): BsonDocument =
    val stamped =
      if document.containsKey(BsonField.Id)
      then document
      else copyOf(document).append(BsonField.Id, BsonObjectId(org.bson.types.ObjectId.get()))

    val id = stamped.get(BsonField.Id)

    if storage.exists(existing => Option(existing.get(BsonField.Id)).exists(BsonOrdering.same(_, id)))
    then
      throw MongoError.DuplicateKey(
        MongoWriteException(WriteError(11000, s"E11000 duplicate key error: _id $id", BsonDocument()), ServerAddress(), java.util.Collections.emptyList())
      )
    else checked(stamped, None)
  end identified

  def find(filter: Filter[E])(using session: Option[ClientSession]): FindQuery[F, S, E] = FakeFindQuery(filter)

  def replaceOne(filter: Filter[E], replacement: E, options: ReplaceOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    atomically {
      requireNoCollation(options.collation, "replaceOne")

      matching(filter).headOption match
        case Some(existing)         =>
          val replaced = replacing(existing, codec.encodeDocument(replacement))
          storage(storage.indexOf(existing)) = checked(replaced, Some(existing))
          UpdateResult(matchedCount = 1, modifiedCount = 1, upsertedId = None)
        case None if options.upsert =>
          val encoded = identified(codec.encodeDocument(replacement))
          storage += encoded
          UpdateResult(matchedCount = 0, modifiedCount = 0, upsertedId = Option(encoded.get(BsonField.Id)))
        case None                   => UpdateResult.none
    }

  def updateOne(filter: Filter[E], update: Update[E], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    atomically {
      requireNoArrayFilters(options.arrayFilters, "updateOne")
      requireNoCollation(options.collation, "updateOne")
      requireRenderable(update)

      matching(filter).headOption match
        case Some(existing)         =>
          val updated = applyUpdate(existing, update)
          storage(storage.indexOf(existing)) = checked(updated, Some(existing))
          UpdateResult(matchedCount = 1, modifiedCount = modifiedBy(existing, updated), upsertedId = None)
        case None if options.upsert =>
          throw UnsupportedOperationException("FakeMongoCollection: updateOne with upsert is not simulated")
        case None                   => UpdateResult.none
    }

  def updateMany(filter: Filter[E], update: Update[E], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    atomically {
      requireNoArrayFilters(options.arrayFilters, "updateMany")
      requireNoCollation(options.collation, "updateMany")
      requireRenderable(update)

      val matches  = matching(filter)
      val modified = matches.map { doc =>
        val updated = applyUpdate(doc, update)
        storage(storage.indexOf(doc)) = checked(updated, Some(doc))
        modifiedBy(doc, updated)
      }
      if matches.isEmpty && options.upsert then throw UnsupportedOperationException("FakeMongoCollection: updateMany with upsert is not simulated")
      UpdateResult(matchedCount = matches.size.toLong, modifiedCount = modified.sum, upsertedId = None)
    }

  def findOneAndUpdate(filter: Filter[E], update: Update[E], options: FindOneAndUpdateOptions[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] =
    atomically {
      requireNoArrayFilters(options.arrayFilters, "findOneAndUpdate")
      requireNoCollation(options.collation, "findOneAndUpdate")
      requireRenderable(update)

      firstMatching(filter, options.sort) match
        case None if options.upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndUpdate with upsert is not simulated")
        case None                   => None
        case Some(existing)         =>
          val updated = applyUpdate(existing, update)
          storage(storage.indexOf(existing)) = checked(updated, Some(existing))
          decodeProjected(if options.returnUpdated then updated else existing, options.projection)
    }

  def findOneAndReplace(filter: Filter[E], replacement: E, options: FindOneAndReplaceOptions[E])(using
      session: Option[ClientSession]
  ): F[Option[E]] =
    atomically {
      requireNoCollation(options.collation, "findOneAndReplace")

      firstMatching(filter, options.sort) match
        case None if options.upsert => throw UnsupportedOperationException("FakeMongoCollection: findOneAndReplace with upsert is not simulated")
        case None                   => None
        case Some(existing)         =>
          val encoded = replacing(existing, codec.encodeDocument(replacement))
          storage(storage.indexOf(existing)) = checked(encoded, Some(existing))
          decodeProjected(if options.returnUpdated then encoded else existing, options.projection)
    }

  def findOneAndDelete(filter: Filter[E], options: FindOneAndDeleteOptions[E])(using session: Option[ClientSession]): F[Option[E]] =
    atomically {
      requireNoCollation(options.collation, "findOneAndDelete")

      firstMatching(filter, options.sort).flatMap { existing =>
        storage -= existing
        decodeProjected(existing, options.projection)
      }
    }

  def deleteOne(filter: Filter[E], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    atomically {
      requireNoCollation(options.collation, "deleteOne")

      matching(filter).headOption match
        case Some(existing) => storage -= existing; DeleteResult(1)
        case None           => DeleteResult.none
    }

  def deleteMany(filter: Filter[E], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    atomically {
      requireNoCollation(options.collation, "deleteMany")

      val matches = matching(filter)
      storage --= matches
      DeleteResult(matches.size.toLong)
    }

  def count(filter: Filter[E], options: CountOptions)(using session: Option[ClientSession]): F[Long] =
    atomically {
      requireNoCollation(options.collation, "count")

      val matches = matching(filter).size.toLong
      val skipped = options.skip.fold(matches)(skip => math.max(0L, matches - skip))
      options.limit.fold(skipped)(limit => math.min(skipped, limit.toLong))
    }

  def estimatedCount: F[Long] = atomically(storage.size.toLong)

  def bulkWrite(commands: Seq[WriteCommand[E]], ordered: Boolean)(using session: Option[ClientSession]): F[BulkWriteResult] =
    atomically {
      var inserted = 0L
      var matched  = 0L
      var modified = 0L
      var deleted  = 0L

      val upserted = mutable.Map.empty[Int, BsonValue]
      val failures = mutable.ListBuffer.empty[BulkWriteError]

      val perform: ((WriteCommand[E], Int)) => Unit = {
        case (WriteCommand.InsertOne(document), _)                 =>
          storage += identified(codec.encodeDocument(document))
          inserted += 1
        case (WriteCommand.ReplaceOne(filter, value, options), i)  =>
          requireNoCollation(options.collation, "bulkWrite ReplaceOne")
          matching(filter).headOption match
            case Some(existing)         =>
              val replaced = replacing(existing, codec.encodeDocument(value))
              storage(storage.indexOf(existing)) = checked(replaced, Some(existing))
              matched += 1
              modified += 1
            case None if options.upsert =>
              val encoded = identified(codec.encodeDocument(value))
              storage += encoded
              Option(encoded.get(BsonField.Id)).foreach(id => upserted.update(i, id))
            case None                   => ()
        case (WriteCommand.UpdateOne(filter, update, options), _)  =>
          requireDefaultUpdateOptions(options, "bulkWrite UpdateOne")
          requireRenderable(update)
          matching(filter).headOption.foreach { doc =>
            val updated = applyUpdate(doc, update)
            storage(storage.indexOf(doc)) = checked(updated, Some(doc))
            matched += 1
            modified += modifiedBy(doc, updated)
          }
        case (WriteCommand.UpdateMany(filter, update, options), _) =>
          requireDefaultUpdateOptions(options, "bulkWrite UpdateMany")
          requireRenderable(update)
          matching(filter).foreach { doc =>
            val updated = applyUpdate(doc, update)
            storage(storage.indexOf(doc)) = checked(updated, Some(doc))
            matched += 1
            modified += modifiedBy(doc, updated)
          }
        case (WriteCommand.DeleteOne(filter), _)                   =>
          matching(filter).headOption.foreach { doc => storage -= doc; deleted += 1 }
        case (WriteCommand.DeleteMany(filter), _)                  =>
          val matches = matching(filter)
          storage --= matches
          deleted += matches.size
      }

      commands.zipWithIndex.iterator.takeWhile(_ => !ordered || failures.isEmpty).foreach { (command, index) =>
        try perform((command, index))
        catch case duplicate: MongoError.DuplicateKey => failures.addOne(failureAt(index, duplicate)): Unit
      }

      val written = BulkWriteResult(inserted, matched, modified, deleted, upserted.toMap)

      if failures.isEmpty
      then written
      else throw bulkFailure(written, failures.toList)
    }

  def aggregate[B](pipeline: Seq[Stage[E]])(using session: Option[ClientSession])(using decoderB: BsonDocumentDecoder[B]): AggregateQuery[F, S, B] =
    FakeAggregateQuery(pipeline.toList, decoderB)

  def distinct[C, B](field: Field[E, C], filter: Filter[E])(using
      session: Option[ClientSession]
  )(using
      @unused values: DistinctOf[C, B],
      decoder: mongo4s.bson.BsonDecoder[B],
  ): DistinctQuery[F, S, B] =
    FakeDistinctQuery(field.path, filter, decoder)

  def createIndex(index: Index[E])(using session: Option[ClientSession]): F[String] =
    F.map(createIndexes(List(index)))(_.head)

  def createIndexes(indexes: Seq[Index[E]])(using session: Option[ClientSession]): F[List[String]] =
    atomically {
      val missing = indexes.filterNot(alreadyBuilt)
      missing.foreach(requireBuildable)

      createdIndexes ++= missing
      indexes.iterator.map(indexName).toList
    }

  private def alreadyBuilt(index: Index[E]): Boolean =
    val name = indexName(index)

    createdIndexes.find(indexName(_) == name) match
      case Some(existing) if existing.keys != index.keys         =>
        throw commandFailure(86, "IndexKeySpecsConflict", s"An existing index has the same name as the requested index but different keys: $name")
      case Some(existing) if !sameSpecification(existing, index) =>
        throw commandFailure(85, "IndexOptionsConflict", s"An existing index has the same name as the requested index but different options: $name")
      case Some(_)                                               => true
      case None                                                  =>
        createdIndexes.find(sameSpecification(_, index)).foreach { existing =>
          throw commandFailure(85, "IndexOptionsConflict", s"Index already exists with a different name: ${indexName(existing)}")
        }
        false
  end alreadyBuilt

  private def sameSpecification(one: Index[E], other: Index[E]): Boolean =
    one.keys == other.keys && one.collation == other.collation && described(one.named("")) == described(other.named(""))

  private def commandFailure(code: Int, codeName: String, message: String): Throwable =
    MongoError.translate(
      MongoCommandException(
        BsonDocument("ok", BsonDouble(0)).append("errmsg", BsonString(message)).append("code", BsonInt32(code)).append("codeName", BsonString(codeName)),
        ServerAddress(),
      )
    )

  private def requireBuildable(index: Index[E]): Unit =
    if index.unique && index.collation.nonEmpty
    then throw UnsupportedOperationException("FakeMongoCollection: a unique index with a collation is not simulated")

    if index.unique
    then storage.toList.foreach(document => requireUnique(index, document, Some(document)))

  private def indexName(index: Index[E]): String =
    index.name.getOrElse {
      index
        .keysToBson(naming)
        .entrySet
        .asScala
        .map { entry =>
          val direction = entry.getValue
          s"${entry.getKey}_${if direction.isNumber then direction.asNumber.intValue.toString else direction.asString.getValue}"
        }
        .mkString("_")
    }

  private def checked(candidate: BsonDocument, replaced: Option[BsonDocument]): BsonDocument =
    createdIndexes.filter(_.unique).foreach(requireUnique(_, candidate, replaced))
    candidate

  private def requireUnique(index: Index[E], candidate: BsonDocument, replaced: Option[BsonDocument]): Unit =
    uniqueKey(index, candidate).foreach { key =>
      val clash = storage.exists { other =>
        !replaced.exists(_ eq other) && uniqueKey(index, other).exists(_.corresponds(key)(BsonOrdering.same))
      }

      if clash
      then
        throw MongoError.DuplicateKey(
          MongoWriteException(
            WriteError(11000, s"E11000 duplicate key error index: ${indexName(index)} dup key: ${key.mkString(", ")}", BsonDocument()),
            ServerAddress(),
            java.util.Collections.emptyList(),
          )
        )
    }

  private def uniqueKey(index: Index[E], document: BsonDocument): Option[List[BsonValue]] =
    val values = index.keys.map((path, _) => at(document, path))

    if index.sparse && values.forall(_.isEmpty)
    then None
    else if index.partialFilter.exists(filter => !matches(document, filter))
    then None
    else if values.exists(_.exists(_.isArray))
    then throw UnsupportedOperationException("FakeMongoCollection: a unique index over an array field is not simulated")
    else Some(values.map(_.getOrElse(BsonNull.VALUE)))
  end uniqueKey

  def listIndexes(using session: Option[ClientSession]): F[List[BsonDocument]] =
    atomically {
      val onId = BsonDocument("v", BsonInt32(2)).append("key", BsonDocument(BsonField.Id, BsonInt32(1))).append("name", BsonString(IdIndex))

      onId :: createdIndexes.map(described).toList
    }

  private def described(index: Index[E]): BsonDocument =
    val description = BsonDocument("v", BsonInt32(2)).append("key", index.keysToBson(naming)).append("name", BsonString(indexName(index)))

    List(
      Option.when(index.unique)("unique" -> BsonBoolean.TRUE),
      Option.when(index.sparse)("sparse" -> BsonBoolean.TRUE),
      index.expireAfter.map(duration => "expireAfterSeconds" -> BsonInt32(duration.toSeconds.toInt)),
      index.partialFilter.map(filter => "partialFilterExpression" -> filter.toBson(naming)),
      Option.when(index.hidden)("hidden" -> BsonBoolean.TRUE),
      index.wildcardProjection.map(projection => "wildcardProjection" -> projection),
    ).flatten.foreach((key, value) => description.append(key, value))

    description
  end described

  def dropIndex(indexName: String)(using session: Option[ClientSession]): F[Unit] =
    atomically {
      if indexName == IdIndex
      then throw commandFailure(72, "InvalidOptions", "cannot drop _id index")

      if !createdIndexes.exists(this.indexName(_) == indexName)
      then throw commandFailure(27, "IndexNotFound", s"index not found with name [$indexName]")

      createdIndexes.filterInPlace(index => this.indexName(index) != indexName)
    }

  def dropIndexes(using session: Option[ClientSession]): F[Unit] = atomically(createdIndexes.clear())

  def renameCollection(to: String, dropTarget: Boolean)(using session: Option[ClientSession]): F[Unit] =
    F.raiseError(UnsupportedOperationException("FakeMongoCollection: renameCollection is not simulated, the fake holds a single namespace"))

  def drop(using session: Option[ClientSession]): F[Unit] = atomically { storage.clear(); createdIndexes.clear() }

  def watchAttempting(options: WatchOptions[E])(using
      session: Option[ClientSession]
  )(using Streamable[S, ChangeEvent[DecodeResult[E]]]): S[ChangeEvent[DecodeResult[E]]] =
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

    case Filter.All(path, values) => elementsAt(document, path).exists(elements => values.forall(value => elements.exists(BsonOrdering.same(_, value))))
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

    case Filter.BitsAllSet(path, mask)   => bitsOf(document, path).exists(bits => (bits & mask) == mask)
    case Filter.BitsAnySet(path, mask)   => bitsOf(document, path).exists(bits => (bits & mask) != 0)
    case Filter.BitsAllClear(path, mask) => bitsOf(document, path).exists(bits => (bits & mask) == 0)
    case Filter.BitsAnyClear(path, mask) => bitsOf(document, path).exists(bits => (bits & mask) != mask)

    case Filter.JsonSchema(_) =>
      throw UnsupportedOperationException("FakeMongoCollection: $jsonSchema validates with the server's schema engine, so it is not simulated")

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
    else leaves(document, path).exists(BsonOrdering.same(_, value))

  private def at(document: BsonDocument, path: mongo4s.FieldPath): Option[BsonValue] =
    def go(current: BsonValue, segments: List[String]): Option[BsonValue] = segments match
      case Nil         => Some(current)
      case seg :: rest => if current.isDocument then Option(current.asDocument.get(seg)).flatMap(go(_, rest)) else None
    go(document, storedSegments(path))
  end at

  private def bitsOf(document: BsonDocument, path: mongo4s.FieldPath): Option[Long] =
    at(document, path).collect {
      case value if value.isInt32 || value.isInt64                    => value.asNumber.longValue
      case value if value.isDouble && value.asDouble.getValue.isWhole => value.asDouble.getValue.toLong
    }

  private def bitwise(current: Option[BsonValue], operation: String, mask: BsonValue): BsonValue =
    val value = current.filterNot(_.isNull).getOrElse(BsonInt32(0))

    if !(value.isInt32 || value.isInt64)
    then
      throw MongoError.translate(
        MongoWriteException(
          WriteError(2, s"Cannot apply $$bit to a value of non-integral type. Has the following type: ${value.getBsonType}", BsonDocument()),
          ServerAddress(),
          java.util.Collections.emptyList(),
        )
      )

    val left   = value.asNumber.longValue
    val right  = mask.asNumber.longValue
    val result = operation match
      case "and" => left & right
      case "or"  => left | right
      case _     => left ^ right

    if value.isInt32 && mask.isInt32 then BsonInt32(result.toInt) else BsonInt64(result)
  end bitwise

  private def applyUpdate(document: BsonDocument, update: Update[E]): BsonDocument = update match
    case Update.Set(path, value) => setAt(document, storedSegments(path), value)
    case Update.Unset(path)      => unsetAt(document, storedSegments(path))
    case Update.Combine(updates) => updates.foldLeft(document)(applyUpdate)

    case Update.SetOnInsert(_, _) => document

    case Update.Inc(path, amount) =>
      setAt(document, storedSegments(path), BsonOrdering.increment(at(document, path), amount))

    case Update.Bit(path, operation, mask) =>
      setAt(document, storedSegments(path), bitwise(at(document, path), operation, mask))

    case Update.Mul(path, factor) =>
      setAt(document, storedSegments(path), BsonOrdering.multiply(at(document, path), factor))

    case Update.Min(path, value) => keepExtreme(document, path, value, _ < 0)
    case Update.Max(path, value) => keepExtreme(document, path, value, _ > 0)

    case Update.CurrentDate(path) =>
      setAt(document, storedSegments(path), BsonDateTime(System.currentTimeMillis()))

    case Update.Rename(path, to) =>
      at(document, path) match
        case None        => document
        case Some(value) => setAt(unsetAt(document, storedSegments(path)), storedSegments(to), value)

    case Update.Push(path, value) => withArray(document, path)(_ :+ value)
    case Update.Pull(path, value) => withArray(document, path)(_.filterNot(_ == value))
    case Update.Pop(path, first)  =>
      withArray(document, path)(elements => if elements.isEmpty then elements else if first then elements.tail else elements.init)

    case Update.PullAll(path, values) =>
      val removed = values.getValues.asScala.toList
      withArray(document, path)(_.filterNot(removed.contains))

    case Update.AddToSet(path, value) =>
      withArray(document, path)(elements => if elements.contains(value) then elements else elements :+ value)

    case Update.AddToSetEach(path, values) =>
      withArray(document, path) { elements =>
        values.getValues.asScala.foldLeft(elements)((acc, value) => if acc.contains(value) then acc else acc :+ value)
      }

    case Update.PushEach(path, values, options) =>
      withArray(document, path)(elements => pushed(elements, values.getValues.asScala.toList, options))

    case raw: Update.Raw[?] =>
      throw UnsupportedOperationException(s"FakeMongoCollection: the raw update ${raw.document.toJson} is not simulated")

    case Update.Pipeline(_) =>
      throw UnsupportedOperationException("FakeMongoCollection: a pipeline update computes from the document on the server, so it is not simulated")

  private def withArray(document: BsonDocument, path: mongo4s.FieldPath)(f: List[BsonValue] => List[BsonValue]): BsonDocument =
    val current = at(document, path) match
      case Some(array) if array.isArray => array.asArray.getValues.asScala.toList
      case Some(other)                  =>
        throw UnsupportedOperationException(s"FakeMongoCollection: an array operator was applied to $other")
      case None                         => Nil

    setAt(document, storedSegments(path), BsonArray(f(current).asJava))
  end withArray

  private def keepExtreme(document: BsonDocument, path: mongo4s.FieldPath, value: BsonValue, keep: Int => Boolean): BsonDocument =
    at(document, path) match
      case Some(current) if !keep(BsonOrdering.compare(value, current)) => document
      case _                                                            => setAt(document, storedSegments(path), value)

  private def pushed(elements: List[BsonValue], added: List[BsonValue], options: PushOptions[?]): List[BsonValue] =
    val placed = options.position match
      case Some(at) => elements.take(at) ++ added ++ elements.drop(at)
      case None     => elements ++ added

    val ordered = options.sortScalars match
      case Some(ascending) => placed.sortWith((a, b) => if ascending then BsonOrdering.compare(a, b) < 0 else BsonOrdering.compare(a, b) > 0)
      case None            =>
        options.sort.fold(placed) { sort =>
          placed.sortWith { (left, right) =>
            sort.fields.view.map { (field, order) =>
              val comparison = (documentAt(left, field), documentAt(right, field)) match
                case (Some(l), Some(r)) => BsonOrdering.compare(l, r)
                case (None, Some(_))    => -1
                case (Some(_), None)    => 1
                case (None, None)       => 0
              if order == SortOrder.Ascending then comparison else -comparison
            }.find(_ != 0).exists(_ < 0)
          }
        }

    options.slice match
      case Some(n) if n >= 0 => ordered.take(n)
      case Some(n)           => ordered.takeRight(-n)
      case None              => ordered
  end pushed

  private def documentAt(value: BsonValue, path: mongo4s.FieldPath): Option[BsonValue] =
    value match
      case document: BsonDocument => at(document, path)
      case _                      => None

  private def decodeProjected(document: BsonDocument, projection: Projection[E]): Option[E] =
    codec.decodeDocument(applyProjection(document, projection)).fold(error => throw error.toThrowable, Some(_))

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
        then Option(document.get(BsonField.Id)).fold(kept)(id => kept.append(BsonField.Id, id))
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
    case Stage.MatchStage(filter)                => documents.filter(matches(_, filter))
    case Stage.SortStage(sort)                   => documents.sortWith(orderedBy(sort))
    case Stage.Skip(n)                           => documents.drop(n)
    case Stage.Limit(n)                          => documents.take(n)
    case Stage.ProjectStage(selected)            => documents.map(applyProjection(_, selected))
    case Stage.Unset(paths)                      => documents.map(document => paths.foldLeft(document)((acc, path) => unsetAt(acc, storedSegments(path))))
    case Stage.Count(fieldName)                  =>
      if documents.isEmpty then Nil else List(BsonDocument(fieldName, BsonInt32(documents.size)))
    case Stage.Group(by, accumulators)           => grouped(documents, by, accumulators)
    case Stage.SortByCount(path)                 => grouped(documents, Some(path), List("count" -> Accumulator.count[E])).sortBy(-_.getInt32("count").getValue)
    case Stage.Fill(partitionBy, sortBy, output) => filled(documents, partitionBy, sortBy, output)
    case other                                   =>
      throw UnsupportedOperationException(s"FakeMongoCollection: ${stageName(other)} is not simulated")

  private def filled(documents: List[BsonDocument], partitionBy: List[mongo4s.FieldPath], sortBy: Sort[E], output: List[FillOutput[E]]) =
    val partitions = documents.groupBy(document => partitionBy.map(at(document, _).getOrElse(BsonNull.VALUE))).values.toList

    partitions.flatMap { partition =>
      val ordered = if sortBy.isEmpty then partition else partition.sortWith(orderedBy(sortBy))
      output.foldLeft(ordered)((rows, method) => fillColumn(rows, method, sortBy))
    }
  end filled

  private def fillColumn(rows: List[BsonDocument], method: FillOutput[E], sortBy: Sort[E]): List[BsonDocument] =
    val segments = storedSegments(method.path)
    val values   = rows.map(row => at(row, method.path).filterNot(_.isNull))

    method match
      case FillOutput.Value(_, value) =>
        rows.zip(values).map((row, current) => if current.isEmpty then setAt(row, segments, value) else row)

      case FillOutput.Locf(_) =>
        rows
          .zip(values)
          .foldLeft((List.empty[BsonDocument], Option.empty[BsonValue])) { case ((done, last), (row, current)) =>
            current match
              case Some(value) => (done :+ row, Some(value))
              case None        => (done :+ setAt(row, segments, last.getOrElse(BsonNull.VALUE)), last)
          }
          ._1

      case FillOutput.Linear(_) =>
        val xs     = rows.map(row => sortBy.fields.headOption.flatMap((path, _) => at(row, path)).map(positionOf))
        val points = values.zip(xs).zipWithIndex.collect { case ((Some(value), Some(x)), index) => (index, x, value.asNumber.doubleValue) }

        rows.zipWithIndex.map { (row, index) =>
          if values(index).nonEmpty
          then row
          else
            val before = points.filter(_._1 < index).lastOption
            val after  = points.find(_._1 > index)

            (before, after, xs(index)) match
              case (Some((_, x0, y0)), Some((_, x1, y1)), Some(x)) => setAt(row, segments, BsonDouble(y0 + (y1 - y0) * (x - x0) / (x1 - x0)))
              case _                                               => setAt(row, segments, BsonNull.VALUE)
        }
    end match
  end fillColumn

  private def positionOf(value: BsonValue): Double =
    if value.isDateTime then value.asDateTime.getValue.toDouble else value.asNumber.doubleValue

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
      accumulators.foldLeft(BsonDocument(BsonField.Id, key)) { (acc, entry) =>
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

    case Accumulator.FirstN(expression, n) => BsonArray(documents.take(n).map(orNull(expression, _)).asJava)
    case Accumulator.LastN(expression, n)  => BsonArray(documents.takeRight(n).map(orNull(expression, _)).asJava)
    case Accumulator.MaxN(expression, n)   => BsonArray(present(expression, documents).sortWith(BsonOrdering.compare(_, _) > 0).take(n).asJava)
    case Accumulator.MinN(expression, n)   => BsonArray(present(expression, documents).sortWith(BsonOrdering.compare(_, _) < 0).take(n).asJava)

    case Accumulator.Top(sortBy, output)        => documents.sortWith(orderedBy(sortBy)).headOption.fold(BsonNull.VALUE: BsonValue)(orNull(output, _))
    case Accumulator.Bottom(sortBy, output)     => documents.sortWith(orderedBy(sortBy)).lastOption.fold(BsonNull.VALUE: BsonValue)(orNull(output, _))
    case Accumulator.TopN(sortBy, output, n)    => BsonArray(documents.sortWith(orderedBy(sortBy)).take(n).map(orNull(output, _)).asJava)
    case Accumulator.BottomN(sortBy, output, n) => BsonArray(documents.sortWith(orderedBy(sortBy)).takeRight(n).map(orNull(output, _)).asJava)

    case Accumulator.StdDevPop(expression)    => deviation(valuesOf(expression, documents), sample = false)
    case Accumulator.StdDevSamp(expression)   => deviation(valuesOf(expression, documents), sample = true)
    case Accumulator.MergeObjects(expression) => merged(valuesOf(expression, documents))

    case approximate @ (_: Accumulator.Median[E] | _: Accumulator.Percentile[E]) =>
      throw UnsupportedOperationException(
        s"FakeMongoCollection: ${approximate.key} is computed by the server's approximate method, which no fake can be faithful to"
      )

  private def orNull(expression: Accumulator.Expression[E], document: BsonDocument): BsonValue =
    valuesOf(expression, List(document)).headOption.getOrElse(BsonNull.VALUE)

  private def present(expression: Accumulator.Expression[E], documents: List[BsonDocument]): List[BsonValue] =
    valuesOf(expression, documents).filterNot(_.isNull)

  private def deviation(values: List[BsonValue], sample: Boolean): BsonValue =
    val numbers = values.filter(_.isNumber).map(_.asNumber.doubleValue)
    val count   = if sample then numbers.size - 1 else numbers.size

    if count < 1
    then BsonNull.VALUE
    else
      val mean = numbers.sum / numbers.size
      BsonDouble(math.sqrt(numbers.map(value => (value - mean) * (value - mean)).sum / count))
  end deviation

  private def merged(values: List[BsonValue]): BsonValue =
    values.filterNot(_.isNull).foldLeft(BsonDocument()) { (acc, value) =>
      if !value.isDocument
      then throw commandFailure(5911200, "Location5911200", s"$$mergeObjects requires object inputs, but input $value is of type ${value.getBsonType}")
      value.asDocument.entrySet.asScala.foreach(entry => acc.put(entry.getKey, entry.getValue): Unit)
      acc
    }

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

    def collation(value: com.mongodb.client.model.Collation): DistinctQuery[F, S, B]        = refuseCollation
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): DistinctQuery[F, S, B] = this
    def batchSize(n: Int): DistinctQuery[F, S, B]                                           = this

    def first: F[Option[B]] = atomically(decoded.headOption)
    def all: F[List[B]]     = atomically(decoded)

    def stream(using Streamable[S, B]): S[B] =
      rs.stream(DeferredPublisher(() => locked(decoded)))

    def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
      def all: F[List[DecodeResult[B]]] = atomically(distinctValues(path, filter).map(decoder.decode))

      def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
        rs.stream(DeferredPublisher(() => locked(distinctValues(path, filter).map(decoder.decode))))

    private def decoded: List[B] =
      distinctValues(path, filter).map(decoder.decode(_).fold(error => throw error.toThrowable, identity))

  private final class FakeAggregateQuery[B](stages: List[Stage[E]], decoderB: BsonDocumentDecoder[B]) extends AggregateQuery[F, S, B]:
    def allowDiskUse(allow: Boolean): AggregateQuery[F, S, B] = this

    def hint(keys: BsonDocument): AggregateQuery[F, S, B]                                    = this
    def collation(value: com.mongodb.client.model.Collation): AggregateQuery[F, S, B]        = refuseCollation
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): AggregateQuery[F, S, B] = this
    def batchSize(n: Int): AggregateQuery[F, S, B]                                           = this
    def comment(value: String): AggregateQuery[F, S, B]                                      = this

    def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
      throw UnsupportedOperationException("FakeMongoCollection: explain needs a real query planner, so it is not simulated")

    def first: F[Option[B]] = atomically(decoded.headOption)
    def all: F[List[B]]     = atomically(decoded)

    def stream(using Streamable[S, B]): S[B] =
      rs.stream(DeferredPublisher(() => locked(decoded)))

    def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
      def all: F[List[DecodeResult[B]]] = atomically(runPipeline(stages).map(decoderB.decodeDocument))

      def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
        rs.stream(DeferredPublisher(() => locked(runPipeline(stages).map(decoderB.decodeDocument))))

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
    def skip(n: Int): FindQuery[F, S, E]                 = FakeFindQuery(filter, projection, ordering, Some(Bounds.notNegative(n, "skip")), limited)
    def limit(n: Int): FindQuery[F, S, E]                = FakeFindQuery(filter, projection, ordering, skipped, Some(Bounds.atLeastOne(n, "limit", Bounds.NoLimit)))

    def hint(keys: BsonDocument): FindQuery[F, S, E]                                    = this
    def collation(value: com.mongodb.client.model.Collation): FindQuery[F, S, E]        = refuseCollation
    def maxTime(duration: scala.concurrent.duration.FiniteDuration): FindQuery[F, S, E] = this
    def batchSize(n: Int): FindQuery[F, S, E]                                           = this
    def comment(value: String): FindQuery[F, S, E]                                      = this

    def explain(verbosity: ExplainVerbosity): F[BsonDocument] =
      throw UnsupportedOperationException("FakeMongoCollection: explain needs a real query planner, so it is not simulated")

    def first: F[Option[E]]                  = atomically(results.headOption)
    def all: F[List[E]]                      = atomically(results)
    def stream(using Streamable[S, E]): S[E] = rs.stream(DeferredPublisher(() => locked(results)))

    def selecting[B](selected: Projection[E], decoder: FieldNaming => BsonDocumentDecoder[B]): SelectQuery[F, S, B] =
      val decode            = decoder(naming).decodeDocument
      val selectedDocuments = () => sourceDocuments.map(applyProjection(_, selected)).map(decode)

      new SelectQuery[F, S, B]:
        def first: F[Option[B]] = atomically(selectedDocuments().headOption.map(orThrow))
        def all: F[List[B]]     = atomically(selectedDocuments().map(orThrow))

        def stream(using Streamable[S, B]): S[B] =
          rs.stream(DeferredPublisher(() => locked(selectedDocuments().map(orThrow))))

        def attempting: DecodeAttempts[F, S, B] = new DecodeAttempts[F, S, B]:
          def all: F[List[DecodeResult[B]]] = atomically(selectedDocuments())

          def stream(using Streamable[S, DecodeResult[B]]): S[DecodeResult[B]] =
            rs.stream(DeferredPublisher(() => locked(selectedDocuments())))
    end selecting

    private def orThrow[B](result: DecodeResult[B]): B = result.fold(error => throw error.toThrowable, identity)

    def attempting: DecodeAttempts[F, S, E] = new DecodeAttempts[F, S, E]:
      def all: F[List[DecodeResult[E]]] = atomically(documents.map(codec.decodeDocument))

      def stream(using Streamable[S, DecodeResult[E]]): S[DecodeResult[E]] = rs.stream(DeferredPublisher(() => locked(documents.map(codec.decodeDocument))))

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
  def same(a: BsonValue, b: BsonValue): Boolean =
    if a.isNumber && b.isNumber then compareNumbers(a, b) == 0 else a == b

  private def compareNumbers(a: BsonValue, b: BsonValue): Int =
    if (a.isInt32 || a.isInt64) && (b.isInt32 || b.isInt64)
    then java.lang.Long.compare(a.asNumber.longValue, b.asNumber.longValue)
    else
      (magnitude(a), magnitude(b)) match
        case (Right(x), Right(y)) => x.compareTo(y)
        case (x, y)               => Integer.compare(x.swap.getOrElse(0), y.swap.getOrElse(0))
  end compareNumbers

  private def magnitude(value: BsonValue): Either[Int, java.math.BigDecimal] =
    if value.isDouble
    then
      val double = value.asDouble.getValue
      if double.isNaN then Left(-2)
      else if double.isInfinite then Left(if double > 0 then 1 else -1)
      else Right(java.math.BigDecimal(double))
    else if value.isDecimal128
    then
      val decimal = value.asDecimal128.getValue
      if decimal.isNaN then Left(-2)
      else if decimal.isInfinite then Left(if decimal.isNegative then -1 else 1)
      else
        Right(try decimal.bigDecimalValue
        catch case _: ArithmeticException => java.math.BigDecimal.ZERO)
    else Right(java.math.BigDecimal.valueOf(value.asNumber.longValue))
  end magnitude

  def compare(a: BsonValue, b: BsonValue): Int =
    if a.isNumber && b.isNumber then compareNumbers(a, b)
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

  def multiply(current: Option[BsonValue], factor: BsonValue): BsonValue =
    val base = current.getOrElse(BsonInt32(0))

    if base.isDecimal128 || factor.isDecimal128
    then throw UnsupportedOperationException("FakeMongoCollection: $mul over a Decimal128 is not simulated")
    else if base.isDouble || factor.isDouble
    then BsonDouble(base.asNumber.doubleValue * factor.asNumber.doubleValue)
    else
      val total = base.asNumber.longValue * factor.asNumber.longValue
      if base.isInt32 && factor.isInt32 && total.isValidInt then BsonInt32(total.toInt) else BsonInt64(total)
  end multiply

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
