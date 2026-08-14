package mongo4s

import com.mongodb.reactivestreams.client.MongoDatabase as RSMongoDatabase
import org.bson.BsonDocument

import mongo4s.bson.direct.WireCodec
import mongo4s.bson.{BsonDocumentCodec, FieldNaming}

import scala.reflect.ClassTag

trait MongoDatabase[F[*], S[*]]:
  def name: String

  def getCollection[A](
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
  )(using BsonDocumentCodec[A]): F[MongoCollection[F, S, A]]

  def getDirectCollection[A](
      collectionName: String,
      naming: FieldNaming = FieldNaming.identity,
  )(using WireCodec[A], ClassTag[A]): F[MongoCollection[F, S, A]]

  def listCollectionNames(using Streamable[S, String]): S[String]
  def listCollections(using Streamable[S, BsonDocument]): S[BsonDocument]

  def createCollection(collectionName: String): F[Unit]
  def runCommand(command: BsonDocument): F[BsonDocument]

  def drop: F[Unit]

  def underlying: RSMongoDatabase
