package mongo4s.repositories

import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.bson.BsonEncoder
import mongo4s.{Field, Streamable}
import mongo4s.operations.{Filter, Index, Update, WriteCommand}
import mongo4s.results.{BulkWriteResult, DeleteResult, UpdateResult}

trait Repository[F[*], S[*], E, K]:
  def count(filter: Filter[E] = Filter.all)(using session: Option[ClientSession] = None): F[Long]

  def exists(key: K)(using session: Option[ClientSession] = None): F[Boolean]

  def findOne(key: K)(using session: Option[ClientSession] = None): F[Option[E]]
  def findMany(keys: List[K])(using session: Option[ClientSession] = None): F[List[E]]

  def findBy[A](
      field: Field[E, A],
      value: A,
      page: Page[E] = Page.all[E],
  )(using
      session: Option[ClientSession] = None
  )(using
      BsonEncoder[A]
  ): F[List[E]]

  def findByFilter(filter: Filter[E], page: Page[E] = Page.all[E])(using session: Option[ClientSession] = None): F[List[E]]

  def getAll(using session: Option[ClientSession] = None)(using Streamable[S, E]): S[E]
  def getBy(filter: Filter[E], page: Page[E] = Page.all[E])(using
      session: Option[ClientSession] = None
  )(using
      Streamable[S, E]
  ): S[E]

  def insertOne(entity: E)(using session: Option[ClientSession] = None): F[Option[org.bson.BsonValue]]
  def insertMany(entities: List[E])(using session: Option[ClientSession] = None): F[List[org.bson.BsonValue]]

  def upsert(entity: E)(using session: Option[ClientSession] = None): F[UpdateResult]
  def upsertMany(entities: List[E])(using session: Option[ClientSession] = None): F[BulkWriteResult]

  def updateField[A](key: K, field: Field[E, A], value: A)(using
      session: Option[ClientSession] = None
  )(using
      BsonEncoder[A]
  ): F[UpdateResult]

  def updateOne(key: K, update: Update[E], upsert: Boolean = false)(using session: Option[ClientSession] = None): F[UpdateResult]
  def updateBy(filter: Filter[E], update: Update[E])(using session: Option[ClientSession] = None): F[UpdateResult]

  def findOneAndUpdate(key: K, update: Update[E], returnUpdated: Boolean = true)(using
      session: Option[ClientSession] = None
  ): F[Option[E]]

  def bulkWrite(commands: Seq[WriteCommand[E]])(using session: Option[ClientSession] = None): F[BulkWriteResult]

  def deleteOne(key: K)(using session: Option[ClientSession] = None): F[DeleteResult]
  def deleteMany(keys: List[K])(using session: Option[ClientSession] = None): F[DeleteResult]
  def deleteBy(filter: Filter[E])(using session: Option[ClientSession] = None): F[DeleteResult]

  def ensureKeyIndex(using session: Option[ClientSession] = None): F[String]

  def createIndex(index: Index[E])(using session: Option[ClientSession] = None): F[String]
