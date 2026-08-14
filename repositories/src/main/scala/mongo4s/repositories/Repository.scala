package mongo4s.repositories

import mongo4s.{Field, Streamable}
import mongo4s.bson.BsonEncoder
import mongo4s.operations.{Filter, Update, WriteCommand}

trait Repository[F[*], S[*], E, K]:
  def count: F[Long]
  def count(filter: Filter[E]): F[Long]

  def findOne(key: K): F[Option[E]]
  def findMany(keys: List[K]): F[List[E]]
  def findBy[A](field: Field[E, A], value: A)(using BsonEncoder[A]): F[List[E]]
  def findByFilter(filter: Filter[E]): F[List[E]]

  def getAll(using Streamable[S, E]): S[E]
  def getBy(filter: Filter[E])(using Streamable[S, E]): S[E]

  def insertOne(entity: E): F[Unit]
  def insertMany(entities: List[E]): F[Unit]

  def upsert(entity: E): F[Unit]
  def upsertMany(entities: List[E]): F[Unit]

  def updateField[A](key: K, field: Field[E, A], value: A)(using BsonEncoder[A]): F[Unit]
  def updateBy(filter: Filter[E], update: Update[E]): F[Long]

  def bulkWrite(commands: Seq[WriteCommand[E]]): F[Unit]

  def deleteOne(key: K): F[Unit]
  def deleteMany(keys: List[K]): F[Unit]
