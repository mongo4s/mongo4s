package mongo4s.repositories

import mongo4s.bson.{BsonDocumentCodec, BsonEncoder, FieldNaming}
import mongo4s.operations.{Filter, Projection, Update, WriteCommand}
import mongo4s.{Effect, Field, MongoCollection, MongoDatabase, PrimaryKey, Streamable, WithId}
import org.bson.types.ObjectId

open class BaseMongoRepository[F[*], S[*], E, K](
    protected val collection: MongoCollection[F, S, E],
    protected val batchSize: Int = BaseMongoRepository.DefaultBatchSize,
    protected val defaultProjection: Projection[E] = Projection.excludeId[E],
)(using F: Effect[F], pk: PrimaryKey[E, K])
    extends Repository[F, S, E, K]:

  def count: F[Long] = collection.count

  def count(filter: Filter[E]): F[Long] = collection.count(filter)

  def findOne(key: K): F[Option[E]] =
    collection.find(pk.eqFilter(key)).projection(defaultProjection).first

  def findMany(keys: List[K]): F[List[E]] =
    batched(keys)(chunk => collection.find(pk.inFilter(chunk)).projection(defaultProjection).all)

  def findBy[A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): F[List[E]] =
    findByFilter(Filter.Eq(field.path, encoder.encode(value)))

  def findByFilter(filter: Filter[E]): F[List[E]] =
    collection.find(filter).projection(defaultProjection).all

  def getAll(using Streamable[S, E]): S[E] = collection.find.projection(defaultProjection).stream

  def getBy(filter: Filter[E])(using Streamable[S, E]): S[E] = collection.find(filter).projection(defaultProjection).stream

  def insertOne(entity: E): F[Unit] = F.map(collection.insertOne(entity))(_ => ())

  def insertMany(entities: List[E]): F[Unit] =
    batched_(entities)(chunk => F.map(collection.insertMany(chunk))(_ => ()))

  def upsert(entity: E): F[Unit] =
    F.map(collection.replaceOne(pk.eqFilter(pk.key(entity)), entity, upsert = true))(_ => ())

  def upsertMany(entities: List[E]): F[Unit] =
    batched_(entities): chunk =>
      collection.bulkWrite(chunk.map(entity => WriteCommand.replaceOne(pk.eqFilter(pk.key(entity)), entity, upsert = true)))

  def updateField[A](key: K, field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): F[Unit] =
    F.map(collection.updateOne(pk.eqFilter(key), Update.Set(field.path, encoder.encode(value))))(_ => ())

  def updateBy(filter: Filter[E], update: Update[E]): F[Long] = collection.updateMany(filter, update)

  def bulkWrite(commands: Seq[WriteCommand[E]]): F[Unit] =
    batched_(commands.toList)(chunk => collection.bulkWrite(chunk))

  def deleteOne(key: K): F[Unit] =
    F.map(collection.deleteOne(pk.eqFilter(key)))(_ => ())

  def deleteMany(keys: List[K]): F[Unit] =
    batched_(keys)(chunk => F.map(collection.deleteMany(pk.inFilter(chunk)))(_ => ()))

  protected def batched[A, B](values: List[A])(f: List[A] => F[List[B]]): F[List[B]] =
    Effect.traverse(values.grouped(batchSize).toList)(f)

  protected def batched_[A](values: List[A])(f: List[A] => F[Unit]): F[Unit] =
    Effect.traverse_(values.grouped(batchSize).toList)(f)

object BaseMongoRepository:
  val DefaultBatchSize: Int = 500

  def create[F[*], S[*], E, K](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using F: Effect[F], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): F[BaseMongoRepository[F, S, E, K]] =
    F.map(database.getCollection[E](collectionName, naming))(BaseMongoRepository(_, batchSize))

  def identified[F[*], S[*], E, K](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using F: Effect[F], codec: BsonDocumentCodec[E], pk: PrimaryKey[E, K]): F[BaseMongoRepository[F, S, E, K]] =
    F.map(database.getCollection[E](collectionName, naming))(BaseMongoRepository(_, batchSize, Projection.empty[E]))

  def objectId[F[*], S[*], E](
      database: MongoDatabase[F, S],
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
      batchSize: Int = DefaultBatchSize,
  )(using
      F: Effect[F],
      codec: BsonDocumentCodec[WithId[ObjectId, E]],
  ): F[BaseMongoRepository[F, S, WithId[ObjectId, E], ObjectId]] =
    F.map(database.getCollection[WithId[ObjectId, E]](collectionName, naming))(BaseMongoRepository(_, batchSize, Projection.empty))
