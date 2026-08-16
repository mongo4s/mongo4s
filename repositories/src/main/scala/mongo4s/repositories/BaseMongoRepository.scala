package mongo4s.repositories

import org.bson.types.ObjectId
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.bson.{BsonDocumentCodec, BsonEncoder, FieldNaming}
import mongo4s.operations.{Filter, Projection, Update, WriteCommand}
import mongo4s.{Effect, Field, MongoCollection, MongoDatabase, PrimaryKey, Streamable, WithId}

open class BaseMongoRepository[F[*], S[*], E, K](
    protected val collection: MongoCollection[F, S, E],
    protected val batchSize: Int = BaseMongoRepository.DefaultBatchSize,
    protected val defaultProjection: Projection[E] = Projection.excludeId[E],
)(using F: Effect[F], pk: PrimaryKey[E, K])
    extends Repository[F, S, E, K]:

  def count(filter: Filter[E])(using session: Option[ClientSession]): F[Long] = collection.count(filter)(using session)

  def findOne(key: K)(using session: Option[ClientSession]): F[Option[E]] =
    collection.find(pk.eqFilter(key))(using session).projection(defaultProjection).first

  def findMany(keys: List[K])(using session: Option[ClientSession]): F[List[E]] =
    batched(keys)(chunk => collection.find(pk.inFilter(chunk))(using session).projection(defaultProjection).all)

  def findBy[A](field: Field[E, A], value: A)(using session: Option[ClientSession])(using encoder: BsonEncoder[A]): F[List[E]] =
    findByFilter(Filter.Eq(field.path, encoder.encode(value)))(using session)

  def findByFilter(filter: Filter[E])(using session: Option[ClientSession]): F[List[E]] =
    collection.find(filter)(using session).projection(defaultProjection).all

  def getAll(using session: Option[ClientSession])(using Streamable[S, E]): S[E] =
    collection.find()(using session).projection(defaultProjection).stream

  def getBy(filter: Filter[E])(using session: Option[ClientSession])(using Streamable[S, E]): S[E] =
    collection.find(filter)(using session).projection(defaultProjection).stream

  def insertOne(entity: E)(using session: Option[ClientSession]): F[Unit] = F.map(collection.insertOne(entity)(using session))(_ => ())

  def insertMany(entities: List[E])(using session: Option[ClientSession]): F[Unit] =
    batched_(entities)(chunk => F.map(collection.insertMany(chunk)(using session))(_ => ()))

  def upsert(entity: E)(using session: Option[ClientSession]): F[Unit] =
    F.map(collection.replaceOne(pk.eqFilter(pk.key(entity)), entity, upsert = true)(using session))(_ => ())

  def upsertMany(entities: List[E])(using session: Option[ClientSession]): F[Unit] =
    batched_(entities): chunk =>
      collection.bulkWrite(chunk.map(entity => WriteCommand.replaceOne(pk.eqFilter(pk.key(entity)), entity, upsert = true)))(using session)

  def updateField[A](key: K, field: Field[E, A], value: A)(using session: Option[ClientSession])(using encoder: BsonEncoder[A]): F[Unit] =
    F.map(collection.updateOne(pk.eqFilter(key), Update.Set(field.path, encoder.encode(value)))(using session))(_ => ())

  def updateBy(filter: Filter[E], update: Update[E])(using session: Option[ClientSession]): F[Long] =
    collection.updateMany(filter, update)(using session)

  def bulkWrite(commands: Seq[WriteCommand[E]])(using session: Option[ClientSession]): F[Unit] =
    batched_(commands.toList)(chunk => collection.bulkWrite(chunk)(using session))

  def deleteOne(key: K)(using session: Option[ClientSession]): F[Unit] =
    F.map(collection.deleteOne(pk.eqFilter(key))(using session))(_ => ())

  def deleteMany(keys: List[K])(using session: Option[ClientSession]): F[Unit] =
    batched_(keys)(chunk => F.map(collection.deleteMany(pk.inFilter(chunk))(using session))(_ => ()))

  protected def batched[A, B](values: List[A])(f: List[A] => F[List[B]]): F[List[B]] =
    Effect.traverse(values.grouped(batchSize).toList)(f)

  protected def batched_[A](values: List[A])(f: List[A] => F[Unit]): F[Unit] =
    Effect.traverse_(values.grouped(batchSize).toList)(f)

object BaseMongoRepository:
  private val DefaultBatchSize: Int = 500

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
