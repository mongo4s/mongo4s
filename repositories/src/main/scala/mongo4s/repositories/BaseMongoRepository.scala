package mongo4s.repositories

import org.bson.BsonValue
import org.bson.types.ObjectId
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.*
import mongo4s.bson.{BsonDocumentCodec, BsonEncoder, FieldNaming}
import mongo4s.results.{BulkWriteResult, DeleteResult, UpdateResult}
import mongo4s.{Effect, Field, MongoCollection, MongoDatabase, PrimaryKey, Streamable, WithId}

open class BaseMongoRepository[F[*], S[*], E, K](
    protected val collection: MongoCollection[F, S, E],
    protected val batchSize: Int = BaseMongoRepository.DefaultBatchSize,
    protected val defaultProjection: Projection[E] = Projection.empty[E],
)(using F: Effect[F], pk: PrimaryKey[E, K])
    extends Repository[F, S, E, K]:

  require(batchSize > 0, s"batchSize must be positive, got $batchSize")

  protected def primaryKey: PrimaryKey[E, K] = pk

  def count(filter: Filter[E])(using session: Option[ClientSession]): F[Long] = collection.count(filter)(using session)

  def exists(key: K)(using session: Option[ClientSession]): F[Boolean] =
    F.map(collection.count(pk.eqFilter(key))(using session))(_ > 0)

  def findOne(key: K)(using session: Option[ClientSession]): F[Option[E]] =
    collection.find(pk.eqFilter(key))(using session).projection(defaultProjection).first

  def findMany(keys: List[K])(using session: Option[ClientSession]): F[List[E]] =
    batched(keys)(chunk => collection.find(pk.inFilter(chunk))(using session).projection(defaultProjection).all)

  def findBy[A](field: Field[E, A], value: A, page: Page[E])(using
      session: Option[ClientSession]
  )(using
      encoder: BsonEncoder[A]
  ): F[List[E]] =
    findByFilter(Filter.Eq(field.path, encoder.encode(value)), page)(using session)

  def findByFilter(filter: Filter[E], page: Page[E])(using session: Option[ClientSession]): F[List[E]] =
    paged(collection.find(filter)(using session).projection(defaultProjection), page).all

  def getAll(using session: Option[ClientSession])(using Streamable[S, E]): S[E] =
    collection.find()(using session).projection(defaultProjection).stream

  def getBy(filter: Filter[E], page: Page[E])(using session: Option[ClientSession])(using Streamable[S, E]): S[E] =
    paged(collection.find(filter)(using session).projection(defaultProjection), page).stream

  def insertOne(entity: E)(using session: Option[ClientSession]): F[Option[BsonValue]] =
    F.map(collection.insertOne(entity)(using session))(_.insertedId)

  def insertMany(entities: List[E])(using session: Option[ClientSession]): F[List[BsonValue]] =
    batched(entities)(chunk => F.map(collection.insertMany(chunk)(using session))(_.insertedIds))

  def upsert(entity: E)(using session: Option[ClientSession]): F[UpdateResult] =
    collection.replaceOne(pk.eqFilter(pk.key(entity)), entity, ReplaceOptions.upsert)(using session)

  def upsertMany(entities: List[E])(using session: Option[ClientSession]): F[BulkWriteResult] =
    batchedResults(entities)(chunk =>
      collection.bulkWrite(chunk.map(entity => WriteCommand.replaceOne(pk.eqFilter(pk.key(entity)), entity, ReplaceOptions.upsert)))(using session)
    )

  def updateField[A](key: K, field: Field[E, A], value: A)(using
      session: Option[ClientSession]
  )(using
      encoder: BsonEncoder[A]
  ): F[UpdateResult] =
    collection.updateOne(pk.eqFilter(key), Update.Set(field.path, encoder.encode(value)))(using session)

  def updateOne(key: K, update: Update[E], options: UpdateOptions)(using session: Option[ClientSession]): F[UpdateResult] =
    collection.updateOne(pk.eqFilter(key), update, options)(using session)

  def updateBy(filter: Filter[E], update: Update[E])(using session: Option[ClientSession]): F[UpdateResult] =
    collection.updateMany(filter, update)(using session)

  def findOneAndUpdate(key: K, update: Update[E], options: FindOneAndUpdateOptions[E])(using session: Option[ClientSession]): F[Option[E]] =
    collection.findOneAndUpdate(pk.eqFilter(key), update, options.withProjection(defaultProjection))(using session)

  def bulkWrite(commands: Seq[WriteCommand[E]])(using session: Option[ClientSession]): F[BulkWriteResult] =
    batchedResults(commands.toList)(chunk => collection.bulkWrite(chunk)(using session))

  def deleteOne(key: K)(using session: Option[ClientSession]): F[DeleteResult] =
    collection.deleteOne(pk.eqFilter(key))(using session)

  def deleteMany(keys: List[K])(using session: Option[ClientSession]): F[DeleteResult] =
    F.map(batchedList(keys)(chunk => collection.deleteMany(pk.inFilter(chunk))(using session)))(results => DeleteResult(results.map(_.deletedCount).sum))

  def deleteBy(filter: Filter[E])(using session: Option[ClientSession]): F[DeleteResult] =
    collection.deleteMany(filter)(using session)

  def ensureKeyIndex(using session: Option[ClientSession]): F[String] =
    collection.createIndex(Index.forKeyFields[E](pk.fieldNames))(using session)

  def createIndex(index: Index[E])(using session: Option[ClientSession]): F[String] =
    collection.createIndex(index)(using session)

  private def paged(query: mongo4s.queries.FindQuery[F, S, E], page: Page[E]): mongo4s.queries.FindQuery[F, S, E] =
    val sorted  = if page.sort.isEmpty then query else query.sort(page.sort)
    val skipped = page.skip.fold(sorted)(sorted.skip)
    page.limit.fold(skipped)(skipped.limit)

  protected def batched[A, B](values: List[A])(f: List[A] => F[List[B]]): F[List[B]] =
    Effect.traverse(values.grouped(batchSize).toList)(f)

  protected def batchedList[A, B](values: List[A])(f: List[A] => F[B]): F[List[B]] =
    Effect.traverse(values.grouped(batchSize).toList)(chunk => F.map(f(chunk))(List(_)))

  protected def batchedResults[A](values: List[A])(f: List[A] => F[BulkWriteResult]): F[BulkWriteResult] =
    val chunks  = values.grouped(batchSize).toList
    val offsets = chunks.scanLeft(0)(_ + _.size)

    F.map {
      Effect.traverse(chunks.zip(offsets)) { (chunk, offset) =>
        F.map(f(chunk))(result => List(result.shiftUpsertedIds(offset)))
      }
    }(BulkWriteResult.combine)
  end batchedResults

object BaseMongoRepository:
  val DefaultBatchSize: Int = 500

  def create[F[*], S[*], E, K](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using F: Effect[F], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): F[BaseMongoRepository[F, S, E, K]] =
    F.map(database.getCollection[E](collectionName, naming)) { collection =>
      BaseMongoRepository(collection, batchSize)
    }

  def withoutId[F[*], S[*], E, K](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using F: Effect[F], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): F[BaseMongoRepository[F, S, E, K]] =
    F.map(database.getCollection[E](collectionName, naming)) { collection =>
      BaseMongoRepository(collection, batchSize, Projection.excludeId[E])
    }

  def objectId[F[*], S[*], E](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using
      F: Effect[F],
      codec: BsonDocumentCodec[WithId[ObjectId, E]],
  ): F[BaseMongoRepository[F, S, WithId[ObjectId, E], ObjectId]] =
    F.map(database.getCollection[WithId[ObjectId, E]](collectionName, naming)) { collection =>
      BaseMongoRepository(collection, batchSize)
    }
