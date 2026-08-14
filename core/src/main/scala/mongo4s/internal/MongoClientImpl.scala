package mongo4s.internal

import org.bson.BsonDocument
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.reactivestreams.client.MongoClient as RSMongoClient

import mongo4s.{Effect, MongoClient, MongoDatabase, RsBridge, Streamable}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoClientImpl[F[*], S[*]](
    val underlying: RSMongoClient,
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoClient[F, S]:

  def getDatabase(name: String): F[MongoDatabase[F, S]] = F.delay(MongoDatabaseImpl(underlying.getDatabase(name)))

  def listDatabaseNames(using Streamable[S, String]): S[String]         = rs.stream(underlying.listDatabaseNames())
  def listDatabases(using Streamable[S, BsonDocument]): S[BsonDocument] = rs.stream(underlying.listDatabases(classOf[BsonDocument]))

  def watch(using Streamable[S, BsonDocument]): S[BsonDocument]                              =
    rs.stream(MappingPublisher(underlying.watch(classOf[BsonDocument]), fullDocument))
  def watch(pipeline: Seq[BsonDocument])(using Streamable[S, BsonDocument]): S[BsonDocument] =
    rs.stream(MappingPublisher(underlying.watch(pipeline.asJava, classOf[BsonDocument]), fullDocument))

  private def fullDocument(event: ChangeStreamDocument[BsonDocument]): BsonDocument = event.getFullDocument

  def close: F[Unit] = F.delay(underlying.close())
