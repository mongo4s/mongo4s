package mongo4s.internal

import org.bson.BsonDocument
import com.mongodb.client.model.*
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.changestream.ChangeEvent
import mongo4s.results.{InsertManyResult, InsertOneResult}
import mongo4s.operations.{Filter, Stage, Update, WriteCommand}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, FieldNaming}
import mongo4s.{Effect, Field, MongoCollection, RsBridge, Streamable}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoCollectionImpl[F[*], S[*], A](
    val underlying: RSMongoCollection[BsonDocument],
    val naming: FieldNaming,
    val codec: BsonDocumentCodec[A],
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoCollection[F, S, A]:

  def name: String = underlying.getNamespace.getCollectionName

  def insertOne(document: A)(using session: Option[ClientSession]): F[InsertOneResult] =
    val encoded   = codec.encodeDocument(document)
    val publisher = session match
      case Some(s) => underlying.insertOne(s, encoded)
      case None    => underlying.insertOne(encoded)
    F.map(rs.one(publisher))(result => InsertOneResult(Option(result.getInsertedId)))

  def insertMany(documents: Seq[A])(using session: Option[ClientSession]): F[InsertManyResult] =
    val documentsList = documents.iterator.map(codec.encodeDocument).toList.asJava
    val publisher     = session match
      case Some(s) => underlying.insertMany(s, documentsList)
      case None    => underlying.insertMany(documentsList)
    F.map(rs.one(publisher))(result => InsertManyResult(result.getInsertedIds.values.asScala.toList))

  def find(filter: Filter[A])(using session: Option[ClientSession]): FindQuery[F, S, A] = query(filter, session)

  def replaceOne(filter: Filter[A], replacement: A, upsert: Boolean)(using session: Option[ClientSession]): F[Long] =
    val options   = ReplaceOptions().upsert(upsert)
    val publisher = session match
      case Some(s) => underlying.replaceOne(s, filter.toBson(naming), codec.encodeDocument(replacement), options)
      case None    => underlying.replaceOne(filter.toBson(naming), codec.encodeDocument(replacement), options)
    F.map(rs.one(publisher))(_.getModifiedCount)

  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean)(using session: Option[ClientSession]): F[Long] =
    val options   = UpdateOptions().upsert(upsert)
    val publisher = session match
      case Some(s) => underlying.updateOne(s, filter.toBson(naming), update.toBson(naming), options)
      case None    => underlying.updateOne(filter.toBson(naming), update.toBson(naming), options)
    F.map(rs.one(publisher))(_.getModifiedCount)

  def updateMany(filter: Filter[A], update: Update[A], upsert: Boolean)(using session: Option[ClientSession]): F[Long] =
    val options   = UpdateOptions().upsert(upsert)
    val publisher = session match
      case Some(s) => underlying.updateMany(s, filter.toBson(naming), update.toBson(naming), options)
      case None    => underlying.updateMany(filter.toBson(naming), update.toBson(naming), options)
    F.map(rs.one(publisher))(_.getModifiedCount)

  def deleteOne(filter: Filter[A])(using session: Option[ClientSession]): F[Long] =
    val publisher = session match
      case Some(s) => underlying.deleteOne(s, filter.toBson(naming))
      case None    => underlying.deleteOne(filter.toBson(naming))
    F.map(rs.one(publisher))(_.getDeletedCount)

  def deleteMany(filter: Filter[A])(using session: Option[ClientSession]): F[Long] =
    val publisher = session match
      case Some(s) => underlying.deleteMany(s, filter.toBson(naming))
      case None    => underlying.deleteMany(filter.toBson(naming))
    F.map(rs.one(publisher))(_.getDeletedCount)

  def count(filter: Filter[A])(using session: Option[ClientSession]): F[Long] =
    val bson      = filter.toBson(naming)
    val publisher = session match
      case Some(s) => underlying.countDocuments(s, bson)
      case None    => underlying.countDocuments(bson)
    F.map(rs.one(publisher))(_.longValue)

  def bulkWrite(commands: Seq[WriteCommand[A]])(using session: Option[ClientSession]): F[Unit] =
    val models    = commands.iterator.map(toModel).toList.asJava
    val publisher = session match
      case Some(s) => underlying.bulkWrite(s, models)
      case None    => underlying.bulkWrite(models)
    rs.unit(publisher)

  def aggregate[B](pipeline: Seq[Stage[A]])(using session: Option[ClientSession])(using codec: BsonDocumentCodec[B]): AggregateQuery[F, S, B] =
    AggregateQueryImpl(underlying, pipeline.map(_.toBson(naming)), codec, None, session)

  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using session: Option[ClientSession])(using decoder: BsonDecoder[B]): DistinctQuery[F, S, B] =
    DistinctQueryImpl(underlying, field.path.render(naming), filter.toBson(naming), decoder, session)

  def watch(pipeline: Seq[BsonDocument], fullDocument: FullDocument)(using
      session: Option[ClientSession]
  )(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]] =
    val base =
      if pipeline.isEmpty then
        session match
          case Some(s) => underlying.watch(s, classOf[BsonDocument])
          case None    => underlying.watch(classOf[BsonDocument])
      else
        session match
          case Some(s) => underlying.watch(s, pipeline.asJava, classOf[BsonDocument])
          case None    => underlying.watch(pipeline.asJava, classOf[BsonDocument])
    rs.stream(DecodingPublisher(base.fullDocument(fullDocument), doc => ChangeEvent.fromDriver(doc, codec.decodeDocument)))

  private def query(filter: Filter[A], session: Option[ClientSession]): FindQuery[F, S, A] =
    FindQueryImpl(underlying, naming, codec, filter, mongo4s.operations.Sort.empty, mongo4s.operations.Projection.empty, None, None, session)

  private def toModel(command: WriteCommand[A]): WriteModel[BsonDocument] = command match
    case WriteCommand.InsertOne(document)            => InsertOneModel(codec.encodeDocument(document))
    case WriteCommand.ReplaceOne(filter, value, up)  => ReplaceOneModel(filter.toBson(naming), codec.encodeDocument(value), ReplaceOptions().upsert(up))
    case WriteCommand.UpdateOne(filter, update, up)  => UpdateOneModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.UpdateMany(filter, update, up) => UpdateManyModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.DeleteOne(filter)              => DeleteOneModel(filter.toBson(naming))
    case WriteCommand.DeleteMany(filter)             => DeleteManyModel(filter.toBson(naming))
