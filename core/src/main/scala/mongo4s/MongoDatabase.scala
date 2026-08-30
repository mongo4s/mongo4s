package mongo4s

import com.mongodb.reactivestreams.client.{ClientSession, MongoDatabase as RSMongoDatabase}
import org.bson.BsonDocument

import mongo4s.bson.direct.WireCodec
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.bson.{BsonDocumentCodec, BsonDocumentDecoder, DecodeResult, FieldNaming}

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

  def listCollectionNames(using session: Option[ClientSession] = None)(using Streamable[S, String]): S[String]
  def listCollections(using session: Option[ClientSession] = None)(using Streamable[S, BsonDocument]): S[BsonDocument]

  def createCollection(collectionName: String)(using session: Option[ClientSession] = None): F[Unit]
  def runCommand(command: BsonDocument)(using session: Option[ClientSession] = None): F[BsonDocument]

  def drop(using session: Option[ClientSession] = None): F[Unit]

  def watch(options: WatchOptions[BsonDocument] = WatchOptions.default[BsonDocument])(using
      session: Option[ClientSession] = None
  )(using Streamable[S, ChangeEvent[BsonDocument]]): S[ChangeEvent[BsonDocument]]

  def watchAs[A](options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]]

  def watchAsAttempting[A](options: WatchOptions[A] = WatchOptions.default[A])(using
      session: Option[ClientSession] = None
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, DecodeResult[ChangeEvent[A]]]): S[DecodeResult[ChangeEvent[A]]]

  def dropCollection(collectionName: String)(using session: Option[ClientSession] = None): F[Unit]

  def underlying: RSMongoDatabase
