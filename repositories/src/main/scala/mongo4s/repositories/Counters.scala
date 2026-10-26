package mongo4s.repositories

import org.bson.{BsonDocument, BsonInt64, BsonString}
import com.mongodb.reactivestreams.client.ClientSession

import mongo4s.operations.*
import mongo4s.bson.{BsonDocumentCodec, BsonError, BsonField}
import mongo4s.{Effect, Field, MongoCollection, MongoDatabase}

trait Counters[F[*]]:
  def next(name: String, step: Long = 1L)(using session: Option[ClientSession] = None): F[Long]
  def current(name: String)(using session: Option[ClientSession] = None): F[Option[Long]]
  def reset(name: String, value: Long = 0L)(using session: Option[ClientSession] = None): F[Unit]

final case class Counter(name: String, value: Long)

object Counter:
  val ValueField: String = "value"

  given codec: BsonDocumentCodec[Counter] =
    BsonDocumentCodec.make(
      counter => BsonDocument(BsonField.Id, BsonString(counter.name)).append(ValueField, BsonInt64(counter.value)),
      document =>
        (Option(document.get(BsonField.Id)), Option(document.get(ValueField))) match
          case (Some(name), Some(value)) if name.isString && value.isNumber => Right(Counter(name.asString.getValue, value.asNumber.longValue))
          case (None, _)                                                    => Left(BsonError.MissingField(BsonField.Id))
          case (_, None)                                                    => Left(BsonError.MissingField(ValueField))
          case _                                                            => Left(BsonError.InvalidValue(s"a counter is a string name and a number, got ${document.toJson}")),
    )

open class MongoCounters[F[*], S[*]](protected val collection: MongoCollection[F, S, Counter])(using F: Effect[F]) extends Counters[F]:

  private val nameField  = Field.stored[Counter, String](BsonField.Id)
  private val valueField = Field.stored[Counter, Long](Counter.ValueField)

  def next(name: String, step: Long)(using session: Option[ClientSession]): F[Long] =
    require(step > 0, s"a counter only moves forward, got a step of $step")

    val advanced = collection.findOneAndUpdate(nameField.equalTo(name), Update.inc(valueField, step), FindOneAndUpdateOptions.default[Counter].withUpsert)(using session)

    F.flatMap(advanced) {
      case Some(counter) => F.pure(counter.value)
      case None          => F.raiseError(IllegalStateException(s"the upsert of counter $name returned no document"))
    }
  end next

  def current(name: String)(using session: Option[ClientSession]): F[Option[Long]] =
    F.map(collection.find(nameField.equalTo(name))(using session).first)(_.map(_.value))

  def reset(name: String, value: Long)(using session: Option[ClientSession]): F[Unit] =
    F.void(collection.updateOne(nameField.equalTo(name), Update.set(valueField, value), UpdateOptions.upsert)(using session))

object MongoCounters:
  val DefaultCollection: String = "counters"

  def create[F[*], S[*]](database: MongoDatabase[F, S], collectionName: String = DefaultCollection)(using F: Effect[F]): F[MongoCounters[F, S]] =
    F.map(database.getCollection[Counter](collectionName))(MongoCounters(_))
