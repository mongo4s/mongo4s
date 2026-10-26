package mongo4s.internal

import scala.annotation.unused
import scala.reflect.ClassTag

import org.bson.BsonDocument
import org.bson.codecs.configuration.CodecRegistries
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}
import com.mongodb.{MongoNamespace, ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client.model.{RenameCollectionOptions, ReplaceOneModel, WriteModel}
import com.mongodb.client.model.{DeleteManyModel, DeleteOneModel, InsertOneModel}

import mongo4s.results.*
import mongo4s.operations.*
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.{DistinctOf, Effect, Field, MongoCollection, RsBridge, Streamable}
import mongo4s.bson.direct.{DocumentCodecBridge, DriverCodecBridge, WireCodec}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, DecodeResult, FieldNaming, BsonDocumentDecoder}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class DirectMongoCollectionImpl[F[*], S[*], A](
    val underlying: RSMongoCollection[BsonDocument],
    val naming: FieldNaming,
)(using F: Effect[F], rs: RsBridge[F, S], wireCodec: WireCodec[A], tag: ClassTag[A])
    extends MongoCollection[F, S, A]:

  lazy val codec: BsonDocumentCodec[A] = DocumentCodecBridge.toDocumentCodec[A]

  private val typedCollection: RSMongoCollection[A] =
    underlying
      .withCodecRegistry(
        CodecRegistries.fromRegistries(
          CodecRegistries.fromCodecs(DriverCodecBridge.toDriverCodec[A]),
          underlying.getCodecRegistry,
        )
      )
      .withDocumentClass(tag.runtimeClass.asInstanceOf[Class[A]])

  def name: String = underlying.getNamespace.getCollectionName

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, A] =
    DirectMongoCollectionImpl(underlying.withReadConcern(concern), naming)

  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, A] =
    DirectMongoCollectionImpl(underlying.withWriteConcern(concern), naming)

  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, A] =
    DirectMongoCollectionImpl(underlying.withReadPreference(preference), naming)

  def insertOne(document: A, options: InsertOneOptions)(using session: Option[ClientSession]): F[InsertOneResult] =
    F.suspend {
      val publisher = session match
        case Some(s) => typedCollection.insertOne(s, document, DriverOptions.insertOne(options))
        case None    => typedCollection.insertOne(document, DriverOptions.insertOne(options))

      F.map(rs.one(publisher)) { result =>
        InsertOneResult.fromDriver(result)
      }
    }

  def insertMany(documents: Seq[A], options: InsertManyOptions)(using session: Option[ClientSession]): F[InsertManyResult] =
    if documents.isEmpty
    then F.pure(InsertManyResult(Nil))
    else
      F.suspend {
        val documentsList = documents.asJava

        val publisher = session match
          case Some(s) => typedCollection.insertMany(s, documentsList, DriverOptions.insertMany(options))
          case None    => typedCollection.insertMany(documentsList, DriverOptions.insertMany(options))

        F.map(rs.one(publisher)) { result =>
          InsertManyResult.fromDriver(result)
        }
      }

  def find(filter: Filter[A])(using session: Option[ClientSession]): FindQuery[F, S, A] = query(filter, session)

  def replaceOne(filter: Filter[A], replacement: A, options: ReplaceOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.replace(options)

      val publisher = session match
        case Some(s) => typedCollection.replaceOne(s, filter.toBson(naming), replacement, driverOptions)
        case None    => typedCollection.replaceOne(filter.toBson(naming), replacement, driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def updateOne(filter: Filter[A], update: Update[A], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.update(options, naming)

      val publisher = DriverUpdates.updateOne(typedCollection, session, filter.toBson(naming), update, naming, driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def updateMany(filter: Filter[A], update: Update[A], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    F.suspend {
      val driverOptions = DriverOptions.update(options, naming)

      val publisher = DriverUpdates.updateMany(typedCollection, session, filter.toBson(naming), update, naming, driverOptions)

      F.map(rs.one(publisher))(UpdateResult.fromDriver)
    }

  def deleteOne(filter: Filter[A], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    F.suspend {
      val driverOptions = DriverOptions.delete(options)

      val publisher = session match
        case Some(s) => typedCollection.deleteOne(s, filter.toBson(naming), driverOptions)
        case None    => typedCollection.deleteOne(filter.toBson(naming), driverOptions)

      F.map(rs.one(publisher)) { result =>
        DeleteResult.fromDriver(result)
      }
    }

  def deleteMany(filter: Filter[A], options: DeleteOptions)(using session: Option[ClientSession]): F[DeleteResult] =
    F.suspend {
      val driverOptions = DriverOptions.delete(options)

      val publisher = session match
        case Some(s) => typedCollection.deleteMany(s, filter.toBson(naming), driverOptions)
        case None    => typedCollection.deleteMany(filter.toBson(naming), driverOptions)

      F.map(rs.one(publisher)) { result =>
        DeleteResult.fromDriver(result)
      }
    }

  def findOneAndUpdate(
      filter: Filter[A],
      update: Update[A],
      options: FindOneAndUpdateOptions[A],
  )(using
      session: Option[ClientSession]
  ): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndUpdate(options, naming)

      val publisher = DriverUpdates.findOneAndUpdate(typedCollection, session, filter.toBson(naming), update, naming, driverOptions)

      rs.option(publisher)
    }

  def findOneAndReplace(filter: Filter[A], replacement: A, options: FindOneAndReplaceOptions[A])(using
      session: Option[ClientSession]
  ): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndReplace(options, naming)

      val publisher = session match
        case Some(s) => typedCollection.findOneAndReplace(s, filter.toBson(naming), replacement, driverOptions)
        case None    => typedCollection.findOneAndReplace(filter.toBson(naming), replacement, driverOptions)

      rs.option(publisher)
    }

  def findOneAndDelete(filter: Filter[A], options: FindOneAndDeleteOptions[A])(using session: Option[ClientSession]): F[Option[A]] =
    F.suspend {
      val driverOptions = DriverOptions.findOneAndDelete(options, naming)

      val publisher = session match
        case Some(s) => typedCollection.findOneAndDelete(s, filter.toBson(naming), driverOptions)
        case None    => typedCollection.findOneAndDelete(filter.toBson(naming), driverOptions)

      rs.option(publisher)
    }

  def count(filter: Filter[A], options: CountOptions)(using session: Option[ClientSession]): F[Long] =
    F.suspend {
      val bson          = filter.toBson(naming)
      val driverOptions = DriverOptions.count(options)

      val publisher = session match
        case Some(s) => underlying.countDocuments(s, bson, driverOptions)
        case None    => underlying.countDocuments(bson, driverOptions)

      F.map(rs.one(publisher))(_.longValue)
    }

  def estimatedCount: F[Long] = F.map(rs.one(underlying.estimatedDocumentCount()))(_.longValue)

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
          case Some(s) => typedCollection.bulkWrite(s, models, driverOptions)
          case None    => typedCollection.bulkWrite(models, driverOptions)

        F.map(rs.one(publisher))(BulkWriteResult.fromDriver)
      }
  end bulkWrite

  override def aggregateDirect[B](pipeline: Seq[Stage[A]])(using
      session: Option[ClientSession],
  )(using
      codecB: WireCodec[B],
      tagB: ClassTag[B],
  ): AggregateQuery[F, S, B] =
    val documentClass = tagB.runtimeClass.asInstanceOf[Class[B]]

    val typed = underlying
      .withCodecRegistry(
        CodecRegistries.fromRegistries(
          CodecRegistries.fromCodecs(DriverCodecBridge.toDriverCodec[B](using codecB, tagB)),
          underlying.getCodecRegistry,
        )
      )
      .withDocumentClass(documentClass)

    DirectAggregateQueryImpl(
      typedCollection = typed,
      documentCollection = underlying,
      documentCodec = DocumentCodecBridge.toDocumentCodec[B](using codecB),
      pipeline = pipeline.map(_.toBson(naming)),
      allowDiskUse = None,
      session = session,
    )
  end aggregateDirect

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

  def watch(options: WatchOptions[A])(using session: Option[ClientSession])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]] =
    rs.liveStream(
      DecodingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.fromDriver(doc, codec.decodeDocument),
      )
    )

  def watchAttempting(options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using Streamable[S, ChangeEvent[DecodeResult[A]]]): S[ChangeEvent[DecodeResult[A]]] =
    rs.liveStream(
      DecodingPublisher(
        ChangeStreamSupport.configure(changeStreamPublisher(options), options),
        doc => ChangeEvent.attempting(doc, codec.decodeDocument),
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

  private def query(filter: Filter[A], session: Option[ClientSession]): FindQuery[F, S, A] =
    DirectFindQueryImpl(
      collection = typedCollection,
      documentCollection = underlying,
      documentCodec = codec,
      naming = naming,
      filter = filter,
      sort = Sort.empty,
      projection = Projection.empty,
      skip = None,
      limit = None,
      session = session,
    )

  private def toModel(command: WriteCommand[A]): WriteModel[A] = command match
    case WriteCommand.InsertOne(document)                 => InsertOneModel(document)
    case WriteCommand.ReplaceOne(filter, value, options)  => ReplaceOneModel(filter.toBson(naming), value, DriverOptions.replace(options))
    case WriteCommand.UpdateOne(filter, update, options)  => DriverUpdates.updateOneModel(filter.toBson(naming), update, naming, DriverOptions.update(options, naming))
    case WriteCommand.UpdateMany(filter, update, options) => DriverUpdates.updateManyModel(filter.toBson(naming), update, naming, DriverOptions.update(options, naming))
    case WriteCommand.DeleteOne(filter)                   => DeleteOneModel(filter.toBson(naming))
    case WriteCommand.DeleteMany(filter)                  => DeleteManyModel(filter.toBson(naming))
