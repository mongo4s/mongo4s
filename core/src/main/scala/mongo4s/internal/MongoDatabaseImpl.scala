package mongo4s.internal

import scala.reflect.ClassTag

import org.bson.BsonDocument
import com.mongodb.reactivestreams.client.{ChangeStreamPublisher, ClientSession, MongoDatabase as RSMongoDatabase}

import mongo4s.bson.direct.WireCodec
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.{Effect, MongoCollection, MongoDatabase, RsBridge, Streamable}
import mongo4s.bson.{BsonDocumentCodec, BsonDocumentDecoder, DecodeResult, FieldNaming}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoDatabaseImpl[F[*], S[*]](
    val underlying: RSMongoDatabase,
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoDatabase[F, S]:

  def name: String = underlying.getName

  def getCollection[A](
      collectionName: String,
      naming: FieldNaming,
  )(using codec: BsonDocumentCodec[A]): F[MongoCollection[F, S, A]] =
    F.delay(
      MongoCollectionImpl(
        underlying.getCollection(collectionName, classOf[BsonDocument]),
        naming,
        codec
      )
    )

  def getDirectCollection[A](
      collectionName: String,
      naming: FieldNaming,
  )(using WireCodec[A], ClassTag[A]): F[MongoCollection[F, S, A]] =
    F.delay(
      DirectMongoCollectionImpl(
        underlying.getCollection(collectionName, classOf[BsonDocument]),
        naming,
      )
    )

  def listCollectionNames(using session: Option[ClientSession])(using Streamable[S, String]): S[String] =
    val publisher = session match
      case Some(s) => underlying.listCollectionNames(s)
      case None    => underlying.listCollectionNames()

    rs.stream(publisher)
  end listCollectionNames

  def listCollections(using session: Option[ClientSession])(using Streamable[S, BsonDocument]): S[BsonDocument] =
    val publisher = session match
      case Some(s) => underlying.listCollections(s, classOf[BsonDocument])
      case None    => underlying.listCollections(classOf[BsonDocument])

    rs.stream(publisher)
  end listCollections

  def createCollection(name: String)(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.createCollection(s, name)
      case None    => underlying.createCollection(name)

    rs.unit(publisher)
  end createCollection

  def runCommand(command: BsonDocument)(using session: Option[ClientSession]): F[BsonDocument] =
    val publisher = session match
      case Some(s) => underlying.runCommand(s, command, classOf[BsonDocument])
      case None    => underlying.runCommand(command, classOf[BsonDocument])

    rs.one(publisher)
  end runCommand

  def drop(using session: Option[ClientSession]): F[Unit] =
    val publisher = session match
      case Some(s) => underlying.drop(s)
      case None    => underlying.drop()

    rs.unit(publisher)
  end drop

  def dropCollection(collectionName: String)(using session: Option[ClientSession]): F[Unit] =
    val collection = underlying.getCollection(collectionName, classOf[BsonDocument])

    val publisher = session match
      case Some(s) => collection.drop(s)
      case None    => collection.drop()

    rs.unit(publisher)
  end dropCollection

  def watch(options: WatchOptions[BsonDocument])(using
      session: Option[ClientSession]
  )(using Streamable[S, ChangeEvent[BsonDocument]]): S[ChangeEvent[BsonDocument]] =
    rs.liveStream(
      DecodingPublisher(
        changeStreamPublisher(options),
        doc => ChangeEvent.fromDriver(doc, Right(_)),
      )
    )

  def watchAs[A](options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]] =
    rs.liveStream(
      DecodingPublisher(
        changeStreamPublisher(options),
        doc => ChangeEvent.fromDriver(doc, decoder.decodeDocument),
      )
    )

  def watchAsAttempting[A](options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, DecodeResult[ChangeEvent[A]]]): S[DecodeResult[ChangeEvent[A]]] =
    rs.liveStream(
      AttemptingPublisher(
        changeStreamPublisher(options),
        doc => ChangeEvent.fromDriver(doc, decoder.decodeDocument),
      )
    )

  private def changeStreamPublisher[E](options: WatchOptions[E])(using
      session: Option[ClientSession]
  ): ChangeStreamPublisher[BsonDocument] =
    val stages = options.pipeline.map(_.toBson(FieldNaming.identity)).toList

    val base =
      if stages.isEmpty
      then
        session match
          case Some(s) => underlying.watch(s, classOf[BsonDocument])
          case None    => underlying.watch(classOf[BsonDocument])
      else
        session match
          case Some(s) => underlying.watch(s, stages.asJava, classOf[BsonDocument])
          case None    => underlying.watch(stages.asJava, classOf[BsonDocument])

    ChangeStreamSupport.configure(base, options)
  end changeStreamPublisher
