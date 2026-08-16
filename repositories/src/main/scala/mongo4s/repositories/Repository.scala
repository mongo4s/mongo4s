package mongo4s.repositories

import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.{Field, Streamable}
import mongo4s.bson.BsonEncoder
import mongo4s.operations.{Filter, Update, WriteCommand}

trait Repository[F[*], S[*], E, K]:
  def count(filter: Filter[E] = Filter.all)(using session: Option[ClientSession] = None): F[Long]

  def findOne(key: K)(using session: Option[ClientSession] = None): F[Option[E]]
  def findMany(keys: List[K])(using session: Option[ClientSession] = None): F[List[E]]
  def findBy[A](field: Field[E, A], value: A)(using session: Option[ClientSession] = None)(using BsonEncoder[A]): F[List[E]]
  def findByFilter(filter: Filter[E])(using session: Option[ClientSession] = None): F[List[E]]

  def getAll(using session: Option[ClientSession] = None)(using Streamable[S, E]): S[E]
  def getBy(filter: Filter[E])(using session: Option[ClientSession] = None)(using Streamable[S, E]): S[E]

  def insertOne(entity: E)(using session: Option[ClientSession] = None): F[Unit]
  def insertMany(entities: List[E])(using session: Option[ClientSession] = None): F[Unit]

  def upsert(entity: E)(using session: Option[ClientSession] = None): F[Unit]
  def upsertMany(entities: List[E])(using session: Option[ClientSession] = None): F[Unit]

  def updateField[A](key: K, field: Field[E, A], value: A)(using session: Option[ClientSession] = None)(using BsonEncoder[A]): F[Unit]
  def updateBy(filter: Filter[E], update: Update[E])(using session: Option[ClientSession] = None): F[Long]

  def bulkWrite(commands: Seq[WriteCommand[E]])(using session: Option[ClientSession] = None): F[Unit]

  def deleteOne(key: K)(using session: Option[ClientSession] = None): F[Unit]
  def deleteMany(keys: List[K])(using session: Option[ClientSession] = None): F[Unit]
