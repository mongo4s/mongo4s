package mongo4s.internal

import org.bson.BsonDocument
import com.mongodb.reactivestreams.client.{ChangeStreamPublisher, ClientSession, MongoClient as RSMongoClient}

import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.bson.{BsonDocumentDecoder, DecodeResult, FieldNaming}
import mongo4s.{Effect, MongoClient, MongoDatabase, RsBridge, Streamable}

import scala.jdk.CollectionConverters.given

private[mongo4s] final class MongoClientImpl[F[*], S[*]](
    val underlying: RSMongoClient,
)(using F: Effect[F], rs: RsBridge[F, S])
    extends MongoClient[F, S]:

  def getDatabase(name: String): F[MongoDatabase[F, S]] = F.delay(MongoDatabaseImpl(underlying.getDatabase(name)))

  def startSession: F[ClientSession] = rs.one(underlying.startSession())

  def listDatabaseNames(using
      session: Option[ClientSession]
  )(using Streamable[S, String]): S[String] =
    val publisher = session match
      case Some(s) => underlying.listDatabaseNames(s)
      case None    => underlying.listDatabaseNames()

    rs.stream(publisher)
  end listDatabaseNames

  def listDatabases(using
      session: Option[ClientSession]
  )(using Streamable[S, BsonDocument]): S[BsonDocument] =
    val publisher = session match
      case Some(s) => underlying.listDatabases(s, classOf[BsonDocument])
      case None    => underlying.listDatabases(classOf[BsonDocument])

    rs.stream(publisher)
  end listDatabases

  def watch(options: WatchOptions[BsonDocument])(using
      session: Option[ClientSession]
  )(using Streamable[S, ChangeEvent[BsonDocument]]): S[ChangeEvent[BsonDocument]] =
    rs.stream(
      DecodingPublisher(
        changeStreamPublisher(options),
        doc => ChangeEvent.fromDriver(doc, Right(_)),
      )
    )

  def watchAs[A](options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, ChangeEvent[A]]): S[ChangeEvent[A]] =
    rs.stream(
      DecodingPublisher(
        changeStreamPublisher(options),
        doc => ChangeEvent.fromDriver(doc, decoder.decodeDocument),
      )
    )

  def watchAsAttempting[A](options: WatchOptions[A])(using
      session: Option[ClientSession]
  )(using decoder: BsonDocumentDecoder[A])(using Streamable[S, DecodeResult[ChangeEvent[A]]]): S[DecodeResult[ChangeEvent[A]]] =
    rs.stream(
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

  def close: F[Unit] = F.delay(underlying.close())
