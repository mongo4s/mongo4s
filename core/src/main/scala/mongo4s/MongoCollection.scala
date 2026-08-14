package mongo4s

import org.bson.BsonDocument
import com.mongodb.reactivestreams.client.MongoCollection as RSMongoCollection

import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, FieldNaming}
import mongo4s.operations.{Filter, Update, WriteCommand}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.results.{InsertManyResult, InsertOneResult}

trait MongoCollection[F[*], S[*], A]:
  def name: String
  def naming: FieldNaming
  def codec: BsonDocumentCodec[A]

  def insertOne(document: A): F[InsertOneResult]
  def insertMany(documents: Seq[A]): F[InsertManyResult]

  def find: FindQuery[F, S, A]
  def find(filter: Filter[A]): FindQuery[F, S, A]

  def replaceOne(filter: Filter[A], replacement: A, upsert: Boolean = false): F[Long]
  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean = false): F[Long]
  def updateMany(filter: Filter[A], update: Update[A], upsert: Boolean = false): F[Long]

  def deleteOne(filter: Filter[A]): F[Long]
  def deleteMany(filter: Filter[A]): F[Long]

  def count: F[Long]
  def count(filter: Filter[A]): F[Long]

  def bulkWrite(commands: Seq[WriteCommand[A]]): F[Unit]

  def aggregate[B](pipeline: Seq[BsonDocument])(using BsonDocumentCodec[B]): AggregateQuery[F, S, B]
  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using BsonDecoder[B]): DistinctQuery[F, S, B]

  def watch(using Streamable[S, BsonDocument]): S[BsonDocument]

  def underlying: RSMongoCollection[BsonDocument]
