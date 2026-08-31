package mongo4s.it.watch

import org.bson.{BsonDocument, BsonInt32}
import org.testcontainers.containers.MongoDBContainer

import mongo4s.bson.BsonDocumentCodec
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec}

object WatchFixture:

  final case class Person(name: String, age: Int) derives WireCodec

  object Person:
    given BsonDocumentCodec[Person] = DocumentCodecBridge.toDocumentCodec[Person]

  val Hello: BsonDocument = BsonDocument().append("hello", BsonInt32(1))

  private lazy val container: MongoDBContainer =
    val instance = new MongoDBContainer("mongo:7")
    instance.start()
    instance

  def connectionString: String = container.getConnectionString
