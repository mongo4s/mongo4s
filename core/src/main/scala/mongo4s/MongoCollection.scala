package mongo4s

import scala.annotation.unused
import scala.reflect.ClassTag

import org.bson.BsonDocument
import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.results.*
import mongo4s.operations.*
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, BsonDocumentDecoder, DecodeResult, FieldNaming}

trait MongoCollection[F[*], S[*], A]:
  def name: String
  def naming: FieldNaming
  def codec: BsonDocumentCodec[A]

  def insertOne(document: A, options: InsertOneOptions = InsertOneOptions.default)(using
      session: Option[ClientSession] = None,
  ): F[InsertOneResult]

  def insertMany(documents: Seq[A], options: InsertManyOptions = InsertManyOptions.default)(using
      session: Option[ClientSession] = None,
  ): F[InsertManyResult]

  def find(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): FindQuery[F, S, A]

  def replaceOne(
      filter: Filter[A],
      replacement: A,
      options: ReplaceOptions = ReplaceOptions.default,
  )(using session: Option[ClientSession] = None): F[UpdateResult]

  def updateOne(
      filter: Filter[A],
      update: Update[A],
      options: UpdateOptions = UpdateOptions.default,
  )(using session: Option[ClientSession] = None): F[UpdateResult]

  def updateMany(
      filter: Filter[A],
      update: Update[A],
      options: UpdateOptions = UpdateOptions.default,
  )(using session: Option[ClientSession] = None): F[UpdateResult]

  def deleteOne(filter: Filter[A], options: DeleteOptions = DeleteOptions.default)(using
      session: Option[ClientSession] = None,
  ): F[DeleteResult]

  def deleteMany(filter: Filter[A], options: DeleteOptions = DeleteOptions.default)(using
      session: Option[ClientSession] = None,
  ): F[DeleteResult]

  def findOneAndUpdate(
      filter: Filter[A],
      update: Update[A],
      options: FindOneAndUpdateOptions[A] = FindOneAndUpdateOptions.default[A],
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def findOneAndReplace(
      filter: Filter[A],
      replacement: A,
      options: FindOneAndReplaceOptions[A] = FindOneAndReplaceOptions.default[A],
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def findOneAndDelete(
      filter: Filter[A],
      options: FindOneAndDeleteOptions[A] = FindOneAndDeleteOptions.default[A],
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def count(
      filter: Filter[A] = Filter.all,
      options: CountOptions = CountOptions.default,
  )(using session: Option[ClientSession] = None): F[Long]
  def estimatedCount: F[Long] = estimatedCount(EstimatedCountOptions.default)
  def estimatedCount(options: EstimatedCountOptions): F[Long]

  def bulkWrite(commands: Seq[WriteCommand[A]], options: BulkWriteOptions = BulkWriteOptions.default)(using
      session: Option[ClientSession] = None,
  ): F[BulkWriteResult]

  def aggregate[B](pipeline: Seq[Stage[A]])(using
      session: Option[ClientSession] = None,
  )(using BsonDocumentDecoder[B]): AggregateQuery[F, S, B]

  def aggregateDirect[B](pipeline: Seq[Stage[A]])(using
      session: Option[ClientSession] = None,
  )(using
      codec: WireCodec[B],
      @unused tag: ClassTag[B],
  ): AggregateQuery[F, S, B] =
    aggregate[B](pipeline)(using session)(using DocumentCodecBridge.toDocumentCodec[B](using codec))

  def distinct[C, B](field: Field[A, C], filter: Filter[A] = Filter.all)(using
      session: Option[ClientSession] = None,
  )(using
      DistinctOf[C, B],
      BsonDecoder[B],
  ): DistinctQuery[F, S, B]

  def createIndex(index: Index[A])(using session: Option[ClientSession] = None): F[String]
  def createIndexes(indexes: Seq[Index[A]])(using session: Option[ClientSession] = None): F[List[String]]
  def listIndexes(using session: Option[ClientSession] = None): F[List[BsonDocument]]
  def dropIndex(name: String)(using session: Option[ClientSession] = None): F[Unit]

  def createSearchIndex(index: SearchIndex[A]): F[String]
  def createSearchIndexes(indexes: Seq[SearchIndex[A]]): F[List[String]]
  def updateSearchIndex(index: SearchIndex[A]): F[Unit]
  def dropSearchIndex(name: String): F[Unit]
  def listSearchIndexes: F[List[BsonDocument]]
  def dropIndexes(using session: Option[ClientSession] = None): F[Unit]

  def renameCollection(to: String, dropTarget: Boolean = false)(using session: Option[ClientSession] = None): F[Unit]
  def drop(using session: Option[ClientSession] = None): F[Unit]

  def watch(options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None,
  )(using
      Streamable[S, ChangeEvent[A]],
  ): S[ChangeEvent[A]]

  def watchAttempting(options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None,
  )(using
      Streamable[S, ChangeEvent[DecodeResult[A]]],
  ): S[ChangeEvent[DecodeResult[A]]]

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, A]
  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, A]
  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, A]

  def underlying: RSMongoCollection[BsonDocument]
