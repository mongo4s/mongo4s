package mongo4s

import org.bson.BsonDocument
import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.operations.{Filter, Index, Projection, ReplaceOptions, Sort, Stage, Update, UpdateOptions, WriteCommand}
import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, DecodeResult, FieldNaming}
import mongo4s.results.{BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult}

trait MongoCollection[F[*], S[*], A]:
  def name: String
  def naming: FieldNaming
  def codec: BsonDocumentCodec[A]

  def insertOne(document: A)(using session: Option[ClientSession] = None): F[InsertOneResult]
  def insertMany(documents: Seq[A])(using session: Option[ClientSession] = None): F[InsertManyResult]

  def find(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): FindQuery[F, S, A]

  def replaceOne(filter: Filter[A], replacement: A, options: ReplaceOptions = ReplaceOptions.default)(using
      session: Option[ClientSession] = None
  ): F[UpdateResult]

  def updateOne(filter: Filter[A], update: Update[A], options: UpdateOptions = UpdateOptions.default)(using
      session: Option[ClientSession] = None
  ): F[UpdateResult]

  def updateMany(filter: Filter[A], update: Update[A], options: UpdateOptions = UpdateOptions.default)(using
      session: Option[ClientSession] = None
  ): F[UpdateResult]

  def deleteOne(filter: Filter[A])(using session: Option[ClientSession] = None): F[DeleteResult]
  def deleteMany(filter: Filter[A])(using session: Option[ClientSession] = None): F[DeleteResult]

  def findOneAndUpdate(
      filter: Filter[A],
      update: Update[A],
      returnUpdated: Boolean = true,
      upsert: Boolean = false,
      sort: Sort[A] = Sort.empty[A],
      projection: Projection[A] = Projection.empty[A],
      arrayFilters: Seq[Filter[?]] = Nil,
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def findOneAndReplace(
      filter: Filter[A],
      replacement: A,
      returnUpdated: Boolean = true,
      upsert: Boolean = false,
      sort: Sort[A] = Sort.empty[A],
      projection: Projection[A] = Projection.empty[A],
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def findOneAndDelete(
      filter: Filter[A],
      sort: Sort[A] = Sort.empty[A],
      projection: Projection[A] = Projection.empty[A],
  )(using session: Option[ClientSession] = None): F[Option[A]]

  def count(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): F[Long]
  def estimatedCount: F[Long]

  def bulkWrite(commands: Seq[WriteCommand[A]], ordered: Boolean = true)(using
      session: Option[ClientSession] = None
  ): F[BulkWriteResult]

  def aggregate[B](pipeline: Seq[Stage[A]])(using
      session: Option[ClientSession] = None
  )(using
      BsonDocumentCodec[B]
  ): AggregateQuery[F, S, B]

  def distinct[B](field: Field[A, B], filter: Filter[A] = Filter.all)(using
      session: Option[ClientSession] = None
  )(using
      BsonDecoder[B]
  ): DistinctQuery[F, S, B]

  def createIndex(index: Index[A])(using session: Option[ClientSession] = None): F[String]
  def listIndexes(using session: Option[ClientSession] = None): F[List[BsonDocument]]
  def dropIndex(name: String)(using session: Option[ClientSession] = None): F[Unit]

  def drop(using session: Option[ClientSession] = None): F[Unit]

  def watch(options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None
  )(using
      Streamable[S, ChangeEvent[A]]
  ): S[ChangeEvent[A]]

  def watchAttempting(options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None
  )(using
      Streamable[S, DecodeResult[ChangeEvent[A]]]
  ): S[DecodeResult[ChangeEvent[A]]]

  def withReadConcern(concern: ReadConcern): MongoCollection[F, S, A]
  def withWriteConcern(concern: WriteConcern): MongoCollection[F, S, A]
  def withReadPreference(preference: ReadPreference): MongoCollection[F, S, A]

  def underlying: RSMongoCollection[BsonDocument]
