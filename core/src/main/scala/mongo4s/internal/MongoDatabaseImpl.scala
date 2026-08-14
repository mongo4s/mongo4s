package mongo4s.internal

import scala.reflect.ClassTag

import org.bson.BsonDocument
import com.mongodb.reactivestreams.client.MongoDatabase as RSMongoDatabase

import mongo4s.bson.direct.WireCodec
import mongo4s.bson.{BsonDocumentCodec, FieldNaming}
import mongo4s.{Effect, MongoCollection, MongoDatabase, RsBridge, Streamable}

private[mongo4s] final class MongoDatabaseImpl[F[*], S[*]](
    val underlying: RSMongoDatabase,
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoDatabase[F, S]:

  def name: String = underlying.getName

  def getCollection[A](
      collectionName: String,
      naming: FieldNaming,
  )(using codec: BsonDocumentCodec[A]): F[MongoCollection[F, S, A]] =
    F.delay(MongoCollectionImpl(underlying.getCollection(collectionName, classOf[BsonDocument]), naming, codec))

  def getDirectCollection[A](
      collectionName: String,
      naming: FieldNaming,
  )(using WireCodec[A], ClassTag[A]): F[MongoCollection[F, S, A]] =
    F.delay(DirectMongoCollectionImpl(underlying.getCollection(collectionName, classOf[BsonDocument]), naming))

  def listCollectionNames(using Streamable[S, String]): S[String]         = rs.stream(underlying.listCollectionNames())
  def listCollections(using Streamable[S, BsonDocument]): S[BsonDocument] = rs.stream(underlying.listCollections(classOf[BsonDocument]))
  def createCollection(name: String): F[Unit]                             = rs.unit(underlying.createCollection(name))
  def runCommand(command: BsonDocument): F[BsonDocument]                  = rs.one(underlying.runCommand(command, classOf[BsonDocument]))
  def drop: F[Unit]                                                       = rs.unit(underlying.drop())
