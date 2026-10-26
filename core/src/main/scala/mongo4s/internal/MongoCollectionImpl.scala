package mongo4s.internal

import scala.annotation.unused

import org.bson.BsonDocument
import com.mongodb.{MongoNamespace, ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client.model.{RenameCollectionOptions, ReplaceOneModel, WriteModel}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}
import com.mongodb.client.model.{DeleteManyModel, DeleteOneModel, InsertOneModel}

import mongo4s.results.*
import mongo4s.operations.*
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.{DistinctOf, Effect, Field, MongoCollection, RsBridge, Streamable}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, DecodeResult, FieldNaming, BsonDocumentDecoder}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoCollectionImpl[F[*], S[*], A](
    val underlying: RSMongoCollection[BsonDocument],
    val naming: FieldNaming,
    val codec: BsonDocumentCodec[A],
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoCollection[F, S, A]:

  def name: String = underlying.getNamespace.getCollectionName

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, A] =
    MongoCollectionImpl(underlying.withReadConcern(concern), naming, codec)

  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, A] =
    MongoCollectionImpl(underlying.withWriteConcern(concern), naming, codec)

  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, A] =
    MongoCollectionImpl(underlying.withReadPreference(preference), naming, codec)

  def insertOne(document: A, options: InsertOneOptions)(using session: Option[ClientSession]): F[InsertOneResult] =
    F.suspend {
      val encoded = codec.encodeDocument(document)

      val publisher = session match
        case Some(s) => underlying.insertOne(s, encoded, DriverOptions.insertOne(options))
        case None    => underlying.insertOne(encoded, DriverOptions.insertOne(options))

      F.map(rs.one(publisher)) { result =>
        InsertOneResult.fromDriver(result)
      }
    }

  def insertMany(documents: Seq[A], options: InsertManyOptions)(using session: Option[ClientSession]): F[InsertManyResult] =
    if documents.isEmpty
    then F.pure(InsertManyResult(Nil))
    else
      F.suspend {
        val encoded = documents.iterator.map(codec.encodeDocument).toList.asJava

        val publisher = session match
          case Some(s) => underlying.insertMany(s, encoded, DriverOptions.insertMany(options))
          case None    => underlying.insertMany(encoded, DriverOptions.insertMany(options))

        F.map(rs.one(publisher)) { result =>
          InsertManyResult.fromDriver(result)
        }
      }

  def find(filter: Filter[A])(using session: Option[ClientSession]): FindQuery[F, S, A] =
    query(filter, session)

  def replaceOne(
      filter: Filter[A],
      replacement: A,
      options: ReplaceOptions,
  )(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.replace(options, naming)

      val publisher = session match
        case Some(s) => underlying.replaceOne(s, filter.toBson(naming), codec.encodeDocument(replacement), driverOptions)
        case None    => underlying.replaceOne(filter.toBson(naming), codec.encodeDocument(replacement), driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def updateOne(filter: Filter[A], update: Update[A], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.update(options, naming)

      val publisher = DriverUpdates.updateOne(underlying, session, filter.toBson(naming), update, naming, driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def updateMany(
      filter: Filter[A],
      update: Update[A],
      options: UpdateOptions,
  )(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.updateMany(options, naming)

      val publisher = DriverUpdates.updateMany(underlying, session, filter.toBson(naming), update, naming, driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def deleteOne(filter: Filter[A], options: DeleteOptions)(using
      session: Option[ClientSession],
  ): F[DeleteResult] =
    F.suspend {
      val driverOptions = DriverOptions.delete(options)

      val publisher = session match
        case Some(s) => underlying.deleteOne(s, filter.toBson(naming), driverOptions)
        case None    => underlying.deleteOne(filter.toBson(naming), driverOptions)

      F.map(rs.one(publisher)) { result =>
        DeleteResult.fromDriver(result)
      }
    }

  def deleteMany(filter: Filter[A], options: DeleteOptions)(using
      session: Option[ClientSession],
  ): F[DeleteResult] =
    F.suspend {
      val driverOptions = DriverOptions.delete(options)

      val publisher = session match
        case Some(s) => underlying.deleteMany(s, filter.toBson(naming), driverOptions)
        case None    => underlying.deleteMany(filter.toBson(naming), driverOptions)

      F.map(rs.one(publisher)) { result =>
        DeleteResult.fromDriver(result)
      }
    }

  def findOneAndUpdate(
      filter: Filter[A],
      update: Update[A],
      options: FindOneAndUpdateOptions[A],
  )(using
      session: Option[ClientSession],
  ): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndUpdate(options, naming)

      val publisher = DriverUpdates.findOneAndUpdate(underlying, session, filter.toBson(naming), update, naming, driverOptions)

      decodeOptional(publisher)
    }

  def findOneAndReplace(
      filter: Filter[A],
      replacement: A,
      options: FindOneAndReplaceOptions[A],
  )(using
      session: Option[ClientSession],
  ): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndReplace(options, naming)

      val encoded = codec.encodeDocument(replacement)

      val publisher = session match
        case Some(s) => underlying.findOneAndReplace(s, filter.toBson(naming), encoded, driverOptions)
        case None    => underlying.findOneAndReplace(filter.toBson(naming), encoded, driverOptions)

      decodeOptional(publisher)
    }

  def findOneAndDelete(filter: Filter[A], options: FindOneAndDeleteOptions[A])(using
      session: Option[ClientSession],
  ): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndDelete(options, naming)

      val publisher = session match
        case Some(s) => underlying.findOneAndDelete(s, filter.toBson(naming), driverOptions)
        case None    => underlying.findOneAndDelete(filter.toBson(naming), driverOptions)

      decodeOptional(publisher)
    }

  def count(filter: Filter[A], options: CountOptions)(using
      session: Option[ClientSession],
  ): F[Long] =
    F.suspend {
      val bson          = filter.toBson(naming)
      val driverOptions = DriverOptions.count(options)

      val publisher = session match
        case Some(s) => underlying.countDocuments(s, bson, driverOptions)
        case None    => underlying.countDocuments(bson, driverOptions)

      F.map(rs.one(publisher))(_.longValue)
    }

  def estimatedCount(options: EstimatedCountOptions): F[Long] =
    F.map(rs.one(underlying.estimatedDocumentCount(DriverOptions.estimatedCount(options))))(_.longValue)

  def bulkWrite(commands: Seq[WriteCommand[A]], options: BulkWriteOptions)(using
      session: Option[ClientSession],
  ): F[BulkWriteResult] =
    if commands.isEmpty
    then F.pure(BulkWriteResult.none)
    else
      F.suspend {
        val models        = commands.iterator.map(toModel).toList.asJava
        val driverOptions = DriverOptions.bulkWrite(options)

        val publisher = session match
          case Some(s) => underlying.bulkWrite(s, models, driverOptions)
          case None    => underlying.bulkWrite(models, driverOptions)

        F.map(rs.one(publisher))(BulkWriteResult.fromDriver)
      }

  def aggregate[B](pipeline: Seq[Stage[A]])(using
      session: Option[ClientSession],
  )(using
      decoder: BsonDocumentDecoder[B],
  ): AggregateQuery[F, S, B] =
    AggregateQueryImpl(
      collection = underlying,
      pipeline = pipeline.map(_.toBson(naming)),
      decoder = decoder,
      allowDiskUse = None,
      session = session,
    )

  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using
      session: Option[ClientSession],
  )(using
      @unused values: DistinctOf[C, B],
      decoder: BsonDecoder[B],
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
      val options = DriverOptions.index(index, naming)

      val keys = index.keysToBson(naming)

      val publisher = session match
        case Some(s) => underlying.createIndex(s, keys, options)
        case None    => underlying.createIndex(keys, options)

      rs.one(publisher)
    }

  def createIndexes(indexes: Seq[Index[A]])(using session: Option[ClientSession]): F[List[String]] =
    if indexes.isEmpty
    then F.pure(Nil)
    else
      F.suspend {
        val models = indexes.iterator.map(DriverOptions.indexModel(_, naming)).toList.asJava

        val publisher = session match
          case Some(s) => underlying.createIndexes(s, models)
          case None    => underlying.createIndexes(models)

        rs.list(publisher)
      }

  def listIndexes(using session: Option[ClientSession]): F[List[BsonDocument]] =
    val publisher = session match
      case Some(s) => underlying.listIndexes(s, classOf[BsonDocument])
      case None    => underlying.listIndexes(classOf[BsonDocument])

    rs.list(publisher)
  end listIndexes

  def createSearchIndex(index: SearchIndex[A]): F[String] = F.map(createSearchIndexes(List(index)))(_.head)

  def createSearchIndexes(indexes: Seq[SearchIndex[A]]): F[List[String]] = SearchIndexes.create(underlying, indexes, naming)

  def updateSearchIndex(index: SearchIndex[A]): F[Unit] = SearchIndexes.update(underlying, index, naming)

  def dropSearchIndex(name: String): F[Unit] = SearchIndexes.drop(underlying, name)

  def listSearchIndexes: F[List[BsonDocument]] = SearchIndexes.list(underlying)

  def dropIndex(indexName: String)(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.dropIndex(s, indexName)
      case None    => underlying.dropIndex(indexName)

    rs.unit(publisher)
  end dropIndex

  def dropIndexes(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.dropIndexes(s)
      case None    => underlying.dropIndexes()

    rs.unit(publisher)
  end dropIndexes

  def renameCollection(to: String, dropTarget: Boolean)(using session: Option[ClientSession]): F[Unit] =
    F.suspend {
      val target  = MongoNamespace(underlying.getNamespace.getDatabaseName, to)
      val options = RenameCollectionOptions().dropTarget(dropTarget)

      val publisher = session match
        case Some(s) => underlying.renameCollection(s, target, options)
        case None    => underlying.renameCollection(target, options)

      rs.unit(publisher)
    }

  def drop(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.drop(s)
      case None    => underlying.drop()

    rs.unit(publisher)
  end drop

  def watch(options: WatchOptions[A])(using
      session: Option[ClientSession],
  )(using
      Streamable[S, ChangeEvent[A]],
  ): S[ChangeEvent[A]] =
    rs.liveStream(
      DecodingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.fromDriver(doc, codec.decodeDocument),
      )
    )

  def watchAttempting(options: WatchOptions[A])(using
      session: Option[ClientSession],
  )(using
      Streamable[S, ChangeEvent[DecodeResult[A]]],
  ): S[ChangeEvent[DecodeResult[A]]] =
    rs.liveStream(
      DecodingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.attempting(doc, codec.decodeDocument),
      )
    )

  private def changeStreamPublisher(options: WatchOptions[A])(using
      session: Option[ClientSession],
  ) =
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
    case WriteCommand.InsertOne(document)                 => InsertOneModel(codec.encodeDocument(document))
    case WriteCommand.ReplaceOne(filter, value, options)  => ReplaceOneModel(filter.toBson(naming), codec.encodeDocument(value), DriverOptions.replace(options, naming))
    case WriteCommand.UpdateOne(filter, update, options)  => DriverUpdates.updateOneModel(filter.toBson(naming), update, naming, DriverOptions.update(options, naming))
    case WriteCommand.UpdateMany(filter, update, options) =>
      DriverUpdates.updateManyModel(filter.toBson(naming), update, naming, DriverOptions.updateMany(options, naming))
    case WriteCommand.DeleteOne(filter, options)          => DeleteOneModel(filter.toBson(naming), DriverOptions.delete(options))
    case WriteCommand.DeleteMany(filter, options)         => DeleteManyModel(filter.toBson(naming), DriverOptions.delete(options))
