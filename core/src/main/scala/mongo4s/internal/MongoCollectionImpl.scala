package mongo4s.internal

import scala.annotation.unused

import org.bson.BsonDocument
import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client.model.{ReplaceOneModel, WriteModel}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}
import com.mongodb.client.model.{BulkWriteOptions, DeleteManyModel, DeleteOneModel, InsertOneModel}

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

  def insertOne(document: A)(using session: Option[ClientSession]): F[InsertOneResult] =
    F.suspend {
      val encoded = codec.encodeDocument(document)

      val publisher = session match
        case Some(s) => underlying.insertOne(s, encoded)
        case None    => underlying.insertOne(encoded)

      F.map(rs.one(publisher)) { result =>
        InsertOneResult.fromDriver(result)
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
      val driverOptions = DriverOptions.replace(options)

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
      val driverOptions = DriverOptions.update(options, naming)

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

  def estimatedCount: F[Long] = F.map(rs.one(underlying.estimatedDocumentCount()))(_.longValue)

  def bulkWrite(commands: Seq[WriteCommand[A]], ordered: Boolean)(using
      session: Option[ClientSession],
  ): F[BulkWriteResult] =
    if commands.isEmpty
    then F.pure(BulkWriteResult.none)
    else
      F.suspend {
        val models  = commands.iterator.map(toModel).toList.asJava
        val options = BulkWriteOptions().ordered(ordered)

        val publisher = session match
          case Some(s) => underlying.bulkWrite(s, models, options)
          case None    => underlying.bulkWrite(models, options)

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
      Streamable[S, DecodeResult[ChangeEvent[A]]],
  ): S[DecodeResult[ChangeEvent[A]]] =
    rs.liveStream(
      AttemptingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.fromDriver(doc, codec.decodeDocument),
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
    case WriteCommand.ReplaceOne(filter, value, options)  => ReplaceOneModel(filter.toBson(naming), codec.encodeDocument(value), DriverOptions.replace(options))
    case WriteCommand.UpdateOne(filter, update, options)  => DriverUpdates.updateOneModel(filter.toBson(naming), update, naming, DriverOptions.update(options, naming))
    case WriteCommand.UpdateMany(filter, update, options) => DriverUpdates.updateManyModel(filter.toBson(naming), update, naming, DriverOptions.update(options, naming))
    case WriteCommand.DeleteOne(filter)                   => DeleteOneModel(filter.toBson(naming))
    case WriteCommand.DeleteMany(filter)                  => DeleteManyModel(filter.toBson(naming))
