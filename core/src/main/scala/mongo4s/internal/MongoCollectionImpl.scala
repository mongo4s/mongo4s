package mongo4s.internal

import org.bson.BsonDocument
import com.mongodb.client.model.*
import com.mongodb.reactivestreams.client.MongoCollection as RSMongoCollection

import mongo4s.operations.{Filter, Update, WriteCommand}
import mongo4s.{Effect, Field, MongoCollection, RsBridge, Streamable}
import mongo4s.results.{InsertManyResult, InsertOneResult}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, FieldNaming}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoCollectionImpl[F[*], S[*], A](
    val underlying: RSMongoCollection[BsonDocument],
    val naming: FieldNaming,
    val codec: BsonDocumentCodec[A],
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoCollection[F, S, A]:

  def name: String = underlying.getNamespace.getCollectionName

  def insertOne(document: A): F[InsertOneResult] =
    F.map(rs.one(underlying.insertOne(codec.encodeDocument(document))))(result => InsertOneResult(Option(result.getInsertedId)))

  def insertMany(documents: Seq[A]): F[InsertManyResult] =
    val documentsList = documents.iterator.map(codec.encodeDocument).toList.asJava
    F.map(rs.one(underlying.insertMany(documentsList)))(result => InsertManyResult(result.getInsertedIds.values.asScala.toList))

  def find: FindQuery[F, S, A]                    = query(Filter.all)
  def find(filter: Filter[A]): FindQuery[F, S, A] = query(filter)

  def replaceOne(filter: Filter[A], replacement: A, upsert: Boolean): F[Long] =
    F.map(rs.one(underlying.replaceOne(filter.toBson(naming), codec.encodeDocument(replacement), ReplaceOptions().upsert(upsert))))(
      _.getModifiedCount
    )

  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean): F[Long] =
    F.map(rs.one(underlying.updateOne(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(upsert))))(_.getModifiedCount)

  def updateMany(filter: Filter[A], update: Update[A], upsert: Boolean): F[Long] =
    F.map(rs.one(underlying.updateMany(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(upsert))))(_.getModifiedCount)

  def deleteOne(filter: Filter[A]): F[Long] =
    F.map(rs.one(underlying.deleteOne(filter.toBson(naming))))(_.getDeletedCount)

  def deleteMany(filter: Filter[A]): F[Long] =
    F.map(rs.one(underlying.deleteMany(filter.toBson(naming))))(_.getDeletedCount)

  def count: F[Long]                    = F.map(rs.one(underlying.countDocuments()))(_.longValue)
  def count(filter: Filter[A]): F[Long] = F.map(rs.one(underlying.countDocuments(filter.toBson(naming))))(_.longValue)

  def bulkWrite(commands: Seq[WriteCommand[A]]): F[Unit] =
    rs.unit(underlying.bulkWrite(commands.iterator.map(toModel).toList.asJava))

  def aggregate[B](pipeline: Seq[BsonDocument])(using codec: BsonDocumentCodec[B]): AggregateQuery[F, S, B] =
    AggregateQueryImpl(underlying, pipeline, codec, None)

  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using decoder: BsonDecoder[B]): DistinctQuery[F, S, B] =
    DistinctQueryImpl(underlying, field.path.render(naming), filter.toBson(naming), decoder)

  def watch(using Streamable[S, BsonDocument]): S[BsonDocument] =
    rs.stream(MappingPublisher(underlying.watch(classOf[BsonDocument]), _.getFullDocument))

  private def query(filter: Filter[A]): FindQuery[F, S, A] =
    FindQueryImpl(underlying, naming, codec, filter, mongo4s.operations.Sort.empty, mongo4s.operations.Projection.empty, None, None)

  private def toModel(command: WriteCommand[A]): WriteModel[BsonDocument] = command match
    case WriteCommand.InsertOne(document)            => InsertOneModel(codec.encodeDocument(document))
    case WriteCommand.ReplaceOne(filter, value, up)  => ReplaceOneModel(filter.toBson(naming), codec.encodeDocument(value), ReplaceOptions().upsert(up))
    case WriteCommand.UpdateOne(filter, update, up)  => UpdateOneModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.UpdateMany(filter, update, up) => UpdateManyModel(filter.toBson(naming), update.toBson(naming), UpdateOptions().upsert(up))
    case WriteCommand.DeleteOne(filter)              => DeleteOneModel(filter.toBson(naming))
    case WriteCommand.DeleteMany(filter)             => DeleteManyModel(filter.toBson(naming))
