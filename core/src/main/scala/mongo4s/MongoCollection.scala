package mongo4s

import org.bson.BsonDocument
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection as RSMongoCollection}

import mongo4s.bson.{BsonDecoder, BsonDocumentCodec, FieldNaming}
import mongo4s.changestream.ChangeEvent
import mongo4s.operations.{Filter, Stage, Update, WriteCommand}
import mongo4s.queries.{AggregateQuery, DistinctQuery, FindQuery}
import mongo4s.results.{InsertManyResult, InsertOneResult}

trait MongoCollection[F[*], S[*], A]:
  def name: String
  def naming: FieldNaming
  def codec: BsonDocumentCodec[A]

  def insertOne(document: A)(using session: Option[ClientSession] = None): F[InsertOneResult]
  def insertMany(documents: Seq[A])(using session: Option[ClientSession] = None): F[InsertManyResult]

  def find(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): FindQuery[F, S, A]

  def replaceOne(filter: Filter[A], replacement: A, upsert: Boolean = false)(using session: Option[ClientSession] = None): F[Long]
  def updateOne(filter: Filter[A], update: Update[A], upsert: Boolean = false)(using session: Option[ClientSession] = None): F[Long]
  def updateMany(filter: Filter[A], update: Update[A], upsert: Boolean = false)(using session: Option[ClientSession] = None): F[Long]

  def deleteOne(filter: Filter[A])(using session: Option[ClientSession] = None): F[Long]
  def deleteMany(filter: Filter[A])(using session: Option[ClientSession] = None): F[Long]

  def count(filter: Filter[A] = Filter.all)(using session: Option[ClientSession] = None): F[Long]

  def bulkWrite(commands: Seq[WriteCommand[A]])(using session: Option[ClientSession] = None): F[Unit]

  def aggregate[B](pipeline: Seq[Stage[A]])(using session: Option[ClientSession] = None)(using BsonDocumentCodec[B]): AggregateQuery[F, S, B]
  def distinct[C, B](field: Field[A, C], filter: Filter[A])(using session: Option[ClientSession] = None)(using BsonDecoder[B]): DistinctQuery[F, S, B]

  def watch(pipeline: Seq[BsonDocument] = Seq.empty, fullDocument: FullDocument = FullDocument.UPDATE_LOOKUP)(using
      session: Option[ClientSession] = None
  )(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]]

  def underlying: RSMongoCollection[BsonDocument]
