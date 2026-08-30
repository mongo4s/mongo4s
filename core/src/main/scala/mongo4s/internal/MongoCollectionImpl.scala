package mongo4s.internal

import java.util.concurrent.TimeUnit

import org.bson.BsonDocument
import com.mongodb.client.model.*
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.{Effect, Field, MongoCollection, RsBridge, Streamable}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, DecodeResult, FieldNaming}
import mongo4s.operations.{Filter, Index, Projection, Sort, Stage, Update, WriteCommand}
import mongo4s.results.{BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoCollectionImpl[F[*], S[*], A](
    val underlying: RSMongoCollection[BsonDocument],
    val naming: FieldNaming,
    val codec: BsonDocumentCodec[A],
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoCollection[F, S, A]:

  def name: String = underlying.getNamespace.getCollectionName

  def insertOne(document: A)(using session: Option[ClientSession]): F[InsertOneResult] =
    F.suspend {
      val encoded = codec.encodeDocument(document)

      val publisher = session match
        case Some(s) => underlying.insertOne(s, encoded)
        case None    => underlying.insertOne(encoded)

      F.map(rs.one(publisher)) { result =>
        InsertOneResult(Option(result.getInsertedId))
      }
    }

  def insertMany(documents: Seq[A])(using session: Option[ClientSession]): F[InsertManyResult] =
    if documents.isEmpty
    then F.pure(InsertManyResult(Nil))
    else
      F.suspend {
        val encoded = documents.iterator.map(codec.encodeDocument).toList.asJava

        val publisher = session match
          case Some(s) => underlying.insertMany(s, encoded)
          case None    => underlying.insertMany(encoded)

        F.map(rs.one(publisher)) { result =>
          InsertManyResult(result.getInsertedIds.values.asScala.toList)
        }
      }

  def find(filter: Filter[A])(using session: Option[ClientSession]): FindQuery[F, S, A] = query(filter, session)

  def replaceOne(
      filter: Filter[A],
      replacement: A,
      upsert: Boolean,
  )(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val options = ReplaceOptions().upsert(upsert)

      val publisher = session match
        case Some(s) => underlying.replaceOne(s, filter.toBson(naming), codec.encodeDocument(replacement), options)
        case None    => underlying.replaceOne(filter.toBson(naming), codec.encodeDocument(replacement), options)

      F.map(rs.one(publisher))(updateResult)
    }

  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean)(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val options = UpdateOptions().upsert(upsert)

      val publisher = session match
        case Some(s) => underlying.updateOne(s, filter.toBson(naming), update.toBson(naming), options)
        case None    => underlying.updateOne(filter.toBson(naming), update.toBson(naming), options)

      F.map(rs.one(publisher))(updateResult)
    }

  def updateMany(
      filter: Filter[A],
      update: Update[A],
      upsert: Boolean,
  )(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val options = UpdateOptions().upsert(upsert)

      val publisher = session match
        case Some(s) => underlying.updateMany(s, filter.toBson(naming), update.toBson(naming), options)
        case None    => underlying.updateMany(filter.toBson(naming), update.toBson(naming), options)

      F.map(rs.one(publisher))(updateResult)
    }

  def deleteOne(filter: Filter[A])(using session: Option[ClientSession]): F[DeleteResult] =
    F.suspend {
      val publisher = session match
        case Some(s) => underlying.deleteOne(s, filter.toBson(naming))
        case None    => underlying.deleteOne(filter.toBson(naming))

      F.map(rs.one(publisher)) { result =>
        DeleteResult(result.getDeletedCount)
      }
    }

  def deleteMany(filter: Filter[A])(using session: Option[ClientSession]): F[DeleteResult] =
    F.suspend {
      val publisher = session match
        case Some(s) => underlying.deleteMany(s, filter.toBson(naming))
        case None    => underlying.deleteMany(filter.toBson(naming))

      F.map(rs.one(publisher)) { result =>
        DeleteResult(result.getDeletedCount)
      }
    }

  def findOneAndUpdate(
      filter: Filter[A],
      update: Update[A],
      returnUpdated: Boolean,
      upsert: Boolean,
      sort: Sort[A],
      projection: Projection[A],
  )(using
      session: Option[ClientSession]
  ): F[Option[A]] =
    F.suspend {
      val options =
        FindOneAndUpdateOptions()
          .upsert(upsert)
          .returnDocument(if returnUpdated then ReturnDocument.AFTER else ReturnDocument.BEFORE)

      if !sort.isEmpty then options.sort(sort.toBson(naming)): Unit
      if !projection.isEmpty then options.projection(projection.toBson(naming)): Unit

      val publisher = session match
        case Some(s) => underlying.findOneAndUpdate(s, filter.toBson(naming), update.toBson(naming), options)
        case None    => underlying.findOneAndUpdate(filter.toBson(naming), update.toBson(naming), options)

      decodeOptional(publisher)
    }

  def findOneAndReplace(
      filter: Filter[A],
      replacement: A,
      returnUpdated: Boolean,
      upsert: Boolean,
      sort: Sort[A],
      projection: Projection[A],
  )(using
      session: Option[ClientSession]
  ): F[Option[A]] =
    F.suspend {
      val options =
        FindOneAndReplaceOptions()
          .upsert(upsert)
          .returnDocument(if returnUpdated then ReturnDocument.AFTER else ReturnDocument.BEFORE)

      if !sort.isEmpty then options.sort(sort.toBson(naming)): Unit
      if !projection.isEmpty then options.projection(projection.toBson(naming)): Unit

      val encoded = codec.encodeDocument(replacement)

      val publisher = session match
        case Some(s) => underlying.findOneAndReplace(s, filter.toBson(naming), encoded, options)
        case None    => underlying.findOneAndReplace(filter.toBson(naming), encoded, options)

      decodeOptional(publisher)
    }

  def findOneAndDelete(filter: Filter[A], sort: Sort[A], projection: Projection[A])(using session: Option[ClientSession]): F[Option[A]] =
    F.suspend {
      val options = FindOneAndDeleteOptions()

      if !sort.isEmpty then options.sort(sort.toBson(naming)): Unit
      if !projection.isEmpty then options.projection(projection.toBson(naming)): Unit

      val publisher = session match
        case Some(s) => underlying.findOneAndDelete(s, filter.toBson(naming), options)
        case None    => underlying.findOneAndDelete(filter.toBson(naming), options)

      decodeOptional(publisher)
    }

  def count(filter: Filter[A])(using session: Option[ClientSession]): F[Long] =
    F.suspend {
      val bson = filter.toBson(naming)

      val publisher = session match
        case Some(s) => underlying.countDocuments(s, bson)
        case None    => underlying.countDocuments(bson)

      F.map(rs.one(publisher))(_.longValue)
    }

  def estimatedCount: F[Long] = F.map(rs.one(underlying.estimatedDocumentCount()))(_.longValue)

  def bulkWrite(commands: Seq[WriteCommand[A]], ordered: Boolean)(using session: Option[ClientSession]): F[BulkWriteResult] =
    if commands.isEmpty
    then F.pure(BulkWriteResult.none)
    else
      F.suspend {
        val models  = commands.iterator.map(toModel).toList.asJava
        val options = BulkWriteOptions().ordered(ordered)

        val publisher = session match
          case Some(s) => underlying.bulkWrite(s, models, options)
          case None    => underlying.bulkWrite(models, options)

        F.map(rs.one(publisher))(bulkResult)
      }

  def aggregate[B](pipeline: Seq[Stage[A]])(using session: Option[ClientSession])(using codec: BsonDocumentCodec[B]): AggregateQuery[F, S, B] =
    AggregateQueryImpl(
      collection = underlying,
      pipeline = pipeline.map(_.toBson(naming)),
      codec = codec,
      allowDiskUse = None,
      session = session,
    )

  def distinct[B](field: Field[A, B], filter: Filter[A])(using
      session: Option[ClientSession]
  )(using
      decoder: BsonDecoder[B]
  ): DistinctQuery[F, S, B] =
    DistinctQueryImpl(
      collection = underlying,
      field = field.path.render(naming),
      filter = filter.toBson(naming),
      decoder = decoder,
      session = session,
    )

  def createIndex(index: Index[A])(using session: Option[ClientSession]): F[String] =
    F.suspend {
      val options = IndexOptions().unique(index.unique).sparse(index.sparse)
      
      index.name.foreach(options.name)
      index.expireAfter.foreach(duration => options.expireAfter(duration.toSeconds, TimeUnit.SECONDS))
      index.partialFilter.foreach(filter => options.partialFilterExpression(filter.toBson(naming)))

      val keys = index.keysToBson(naming)

      val publisher = session match
        case Some(s) => underlying.createIndex(s, keys, options)
        case None    => underlying.createIndex(keys, options)

      rs.one(publisher)
    }

  def listIndexes(using session: Option[ClientSession]): F[List[BsonDocument]] =
    val publisher = session match
      case Some(s) => underlying.listIndexes(s, classOf[BsonDocument])
      case None    => underlying.listIndexes(classOf[BsonDocument])

    rs.list(publisher)
  end listIndexes

  def dropIndex(indexName: String)(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.dropIndex(s, indexName)
      case None    => underlying.dropIndex(indexName)

    rs.unit(publisher)
  end dropIndex

  def drop(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.drop(s)
      case None    => underlying.drop()

    rs.unit(publisher)
  end drop

  def watch(options: WatchOptions[A])(using session: Option[ClientSession])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]] =
    rs.stream(
      DecodingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.fromDriver(doc, codec.decodeDocument),
      )
    )

  def watchAttempting(options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using Streamable[S, DecodeResult[ChangeEvent[A]]]): S[DecodeResult[ChangeEvent[A]]] =
    rs.stream(
      AttemptingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.fromDriver(doc, codec.decodeDocument),
      )
    )

  private def changeStreamPublisher(options: WatchOptions[A])(using session: Option[ClientSession]) =
    val stages = options.pipeline.map(_.toBson(naming)).toList

    if stages.isEmpty
    then
      session match
        case Some(s) => underlying.watch(s, classOf[BsonDocument])
        case None    => underlying.watch(classOf[BsonDocument])
    else
      session match
        case Some(s) => underlying.watch(s, stages.asJava, classOf[BsonDocument])
        case None    => underlying.watch(stages.asJava, classOf[BsonDocument])
  end changeStreamPublisher

  private def decodeOptional(publisher: org.reactivestreams.Publisher[BsonDocument]): F[Option[A]] =
    F.flatMap(rs.option(publisher)) {
      case None           => F.pure(None)
      case Some(document) => F.map(Effect.fromEither(codec.decodeDocument(document)))(Some(_))
    }

  private def updateResult(result: com.mongodb.client.result.UpdateResult): UpdateResult =
    UpdateResult(
      matchedCount = result.getMatchedCount,
      modifiedCount = result.getModifiedCount,
      upsertedId = Option(result.getUpsertedId),
    )

  private def bulkResult(result: com.mongodb.bulk.BulkWriteResult): BulkWriteResult =
    BulkWriteResult(
      insertedCount = result.getInsertedCount.toLong,
      matchedCount = result.getMatchedCount.toLong,
      modifiedCount = result.getModifiedCount.toLong,
      deletedCount = result.getDeletedCount.toLong,
      upsertedIds = result.getUpserts.asScala.map(upsert => upsert.getIndex -> upsert.getId).toMap,
    )

  private def query(filter: Filter[A], session: Option[ClientSession]): FindQuery[F, S, A] =
    FindQueryImpl(
      collection = underlying,
      naming = naming,
      codec = codec,
      filter = filter,
      sort = Sort.empty,
      projection = Projection.empty,
      skip = None,
      limit = None,
      session = session,
    )

  private def toModel(command: WriteCommand[A]): WriteModel[BsonDocument] = command match
    case WriteCommand.InsertOne(document)            => InsertOneModel(codec.encodeDocument(document))
    case WriteCommand.ReplaceOne(filter, value, up)  => ReplaceOneModel(filter.toBson(naming), codec.encodeDocument(value), ReplaceOptions().upsert(up))
    case WriteCommand.UpdateOne(filter, update, up)  => UpdateOneModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.UpdateMany(filter, update, up) => UpdateManyModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.DeleteOne(filter)              => DeleteOneModel(filter.toBson(naming))
    case WriteCommand.DeleteMany(filter)             => DeleteManyModel(filter.toBson(naming))
