package mongo4s.changestream

import org.bson.BsonDocument

final case class Namespace(database: String, collection: Option[String])

object Namespace:
  private val DatabaseKey   = "db"
  private val CollectionKey = "coll"

  private[mongo4s] def fromDriver(document: BsonDocument): Option[Namespace] =
    Option(document).flatMap { namespace =>
      Option(namespace.get(DatabaseKey)).filter(_.isString).map { database =>
        Namespace(database.asString.getValue, Option(namespace.get(CollectionKey)).filter(_.isString).map(_.asString.getValue))
      }
    }
