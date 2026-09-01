package mongo4s.examples

import cats.effect.IO
import org.bson.types.ObjectId
import org.bson.{BsonDocument, BsonInt32, BsonString, BsonTimestamp}
import com.mongodb.WriteConcern
import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.FullDocument

import mongo4s.cats.{CatsStream, MongoClientResource}
import mongo4s.results.{BulkWriteResult, UpdateResult}
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.repositories.{BaseMongoRepository, Page}
import mongo4s.bson.{BsonDocumentCodec, DecodeResult, FieldNaming}
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec, WireCodecConfig}
import mongo4s.operations.{Accumulator, Filter, Index, Projection, Sort, Stage, Update, WriteCommand}
import mongo4s.{Field, MongoClient, MongoCollection, MongoDatabase, PrimaryKey, RsBridgeConfig, WithId, withTransaction}

import scala.concurrent.duration.given
import mongo4s.cats.CatsInstances.given
import mongo4s.bson.BsonInstances.given

object ReadmeSnippets:

  private type S[A] = CatsStream[IO][A]

  final case class User(id: String, name: String, age: Int, tags: List[String], score: Option[Long]) derives WireCodec

  object User:
    given PrimaryKey[User, String] = PrimaryKey.single("id")(_.id)
    given BsonDocumentCodec[User]  = DocumentCodecBridge.toDocumentCodec[User]

  final case class Address(city: String, zip: String) derives WireCodec
  final case class Item(sku: String, quantity: Int) derives WireCodec
  final case class Order(userId: String, seq: Int, address: Address, items: List[Item], totals: Map[String, Int]) derives WireCodec

  object Order:
    given PrimaryKey[Order, (String, Int)] =
      PrimaryKey.compound[Order, (String, Int), String, Int](o => (o.userId, o.seq))("user_id", _._1)("seq", _._2)

  // --- Quick start ---

  def quickStart: IO[Unit] =
    MongoClientResource.fromConnectionString[IO]("mongodb://localhost:27017").use { client =>
      for
        db         <- client.getDatabase("myapp")
        collection <- db.getDirectCollection[User]("users")
        users       = BaseMongoRepository(collection)
        _          <- users.insertOne(User("1", "Alice", 30, Nil, None))
        alice      <- users.findOne("1")
        adults     <- users.findByFilter(Field.of[User, Int](_.age).gte(18))
      yield println(s"$alice, ${adults.size} adults")
    }

  // --- Field selectors ---

  val adults = Field.of[User, Int](_.age).gte(18)
  val named  = Field.of[User, String](_.name).equalTo("Alice") && adults
  val city   = Field.of[Order, String](_.address.city).equalTo("Berlin")

  // --- Filters ---

  val ageField   = Field.of[User, Int](_.age)
  val tagsField  = Field.of[User, List[String]](_.tags)
  val scoreField = Field.of[User, Option[Long]](_.score)
  val itemsField = Field.of[Order, List[Item]](_.items)

  val urgent    = tagsField.contains("urgent")
  val everyTag  = tagsField.containsAll(List("urgent", "vip"))
  val threeTags = tagsField.hasSize(3)
  val teens     = ageField.gte(13) && ageField.lte(19)
  val notListed = ageField.notIn(List(40, 41, 42))

  // One element has to satisfy every condition, which `&&` on the array path alone does not require.
  val bigOrder = itemsField.elemMatch(
    Field.of[Item, String](_.sku).equalTo("abc") && Field.of[Item, Int](_.quantity).gt(2)
  )

  val search   = Filter.text[User]("scala")
  val sameAs   = Filter.expr[User](BsonDocument("$gt", BsonDocument()))
  val startsA  = Field.of[User, String](_.name).regex("^A")
  val hasScore = scoreField.exists

  // --- Stored paths: map keys, array positions, _id ---

  val totals                    = Field.of[Order, Map[String, Int]](_.totals)
  val eur                       = totals.at("EUR")
  val first: Field[Order, Item] = itemsField / "0"
  val orderId                   = Field.stored[Order, ObjectId]("_id")

  // --- Updates ---

  val setAge   = Field.of[User, Int](_.age).set(31)
  val birthday = ageField.inc(1)
  val bestYet  = scoreField.max(100L) // an Option[Long] field takes a plain Long
  val tagged   = tagsField.push("vip")
  val combined = Update.combine(setAge, birthday, tagged)

  // Raw carries operators the AST does not model, and merges with the typed ones rather than replacing them.
  val withBit = Update
    .set(Field.of[User, String](_.name), "bob")
    .and(
      Update.Raw[User](BsonDocument("$bit", BsonDocument("age", BsonDocument("and", BsonString("7")))))
    )

  // --- Array elements ---

  def resetEveryQuantity(collection: MongoCollection[IO, S, Order]): IO[UpdateResult] =
    val everyQuantity: Field[Order, Int] = Field.of[Order, List[Item]](_.items) / "$[]" / "quantity"

    collection.updateOne(Field.of[Order, Int](_.seq).equalTo(1), Update.set(everyQuantity, 0))

  def raiseLowQuantities(collection: MongoCollection[IO, S, Order]): IO[UpdateResult] =
    val lowQuantity: Field[Order, Int]    = Field.of[Order, List[Item]](_.items) / "$[low]" / "quantity"
    val elementQuantity: Field[Item, Int] = Field.stored("low.quantity")

    collection.updateOne(
      Field.of[Order, Int](_.seq).equalTo(1),
      Update.set(lowQuantity, 100),
      arrayFilters = Seq(elementQuantity.lt(3)),
    )

  // --- Concerns ---

  def durableWrite(collection: MongoCollection[IO, S, User], user: User, other: User): IO[Unit] =
    val durable = collection.withWriteConcern(WriteConcern.MAJORITY)
    durable.insertOne(user) *> collection.insertOne(other).void

  // --- Results ---

  def upsertedId(collection: MongoCollection[IO, S, User]): IO[Option[org.bson.BsonValue]] =
    collection.updateOne(named, setAge, upsert = true).map(r => if r.wasUpserted then r.upsertedId else None)

  def bulk(collection: MongoCollection[IO, S, User]): IO[BulkWriteResult] =
    collection.bulkWrite(
      Seq(
        WriteCommand.InsertOne(User("2", "Bob", 41, Nil, None)),
        WriteCommand.updateOne(named, setAge),
        WriteCommand.DeleteMany(ageField.lt(0)),
      )
    )

  // --- Queries: options, first/all/stream ---

  def tunedQuery(collection: MongoCollection[IO, S, User]): IO[List[User]] =
    collection
      .find(adults)
      .sort(Sort.asc(Field.of[User, String](_.name)))
      .projection(Projection.empty[User].include(ageField).withoutId)
      .skip(20)
      .limit(10)
      .hint(BsonDocument("age", BsonString("1")))
      .collation(Collation.builder().locale("en").build())
      .maxTime(5.seconds)
      .batchSize(100)
      .comment("adults page 3")
      .all

  def names(collection: MongoCollection[IO, S, User]): IO[List[String]] =
    collection.distinct(Field.of[User, String](_.name)).all

  // --- Lenient decoding ---

  def readable(collection: MongoCollection[IO, S, User]): IO[List[User]] =
    collection.find().attempting.all.map(_.collect { case Right(user) => user })

  def reportBroken(collection: MongoCollection[IO, S, User]): S[DecodeResult[User]] =
    collection.find().attempting.stream

  // --- Sessions and transactions ---

  def transactions(client: MongoClient[IO, S], users: BaseMongoRepository[IO, S, User, String]): IO[Unit] =
    client.withTransaction {
      users.insertOne(User("2", "Bob", 41, Nil, None)) *> users.insertOne(User("3", "Carol", 29, Nil, None)).void
    }

  def manualSession(client: MongoClient[IO, S], users: BaseMongoRepository[IO, S, User, String]): IO[Unit] =
    for
      session <- client.startSession
      _       <- session.withTransaction(users.insertOne(User("4", "Dan", 33, Nil, None)))
      _       <- IO.delay(session.close())
    yield ()

  // --- Aggregation ---

  val pipeline = Seq(
    Stage.matching(adults),
    Stage.sortBy(Sort.asc(Field.of[User, String](_.name))),
    Stage.limit(10),
  )

  val byAge = Seq(
    Stage.groupBy(ageField)(
      "count" -> Accumulator.count[User],
      "names" -> Accumulator.push(Field.of[User, String](_.name)),
    )
  )

  val split = Seq(
    Stage.facet[User](
      "adults" -> List(Stage.matching(adults), Stage.count("n")),
      "minors" -> List(Stage.matching(ageField.lt(18)), Stage.count("n")),
    )
  )

  def aggregated(collection: MongoCollection[IO, S, User]): IO[List[User]] =
    collection.aggregate[User](pipeline).allowDiskUse(true).maxTime(10.seconds).all

  // --- Indexes ---

  def indexes(collection: MongoCollection[IO, S, User]): IO[List[BsonDocument]] =
    for
      _      <- collection.createIndex(Index.ascending(Field.of[User, String](_.name)).descending(ageField).named("name_age"))
      _      <- collection.createIndex(Index.unique(Field.of[User, String](_.id)))
      _      <- collection.createIndex(Index.ascending(ageField).expiringAfter(30.days))
      _      <- collection.createIndex(Index.ascending(ageField).where(adults))
      _      <- collection.createIndex(Index.empty[User].text(Field.of[User, String](_.name)).withSparse)
      listed <- collection.listIndexes
      _      <- collection.dropIndex("name_age")
    yield listed

  // --- Aggregation: a stage's own output is named raw ---

  val buckets = Seq(
    Stage.groupBy(ageField)("count" -> Accumulator.count[User]),
    Stage.raw[User](BsonDocument("$sort", BsonDocument("_id", BsonInt32(1)))),
  )

  def rawShape(collection: MongoCollection[IO, S, User]): IO[Option[BsonDocument]] =
    collection.aggregate[BsonDocument](split).first

  // --- Repositories: pagination and the key index ---

  def indexAndPage(users: BaseMongoRepository[IO, S, User, String]): IO[List[User]] =
    for
      _      <- users.ensureKeyIndex
      result <- users.findByFilter(adults, Page.sortedBy(Sort.asc(Field.of[User, String](_.name))).skipping(20).taking(10))
    yield result

  def pagedStream(users: BaseMongoRepository[IO, S, User, String]): S[User] =
    users.getBy(adults, Page.first(100))

  def atomically(users: BaseMongoRepository[IO, S, User, String]): IO[Option[User]] =
    users.findOneAndUpdate("1", birthday, returnUpdated = true)

  // --- Change streams ---

  def allEvents(collection: MongoCollection[IO, S, User]): S[ChangeEvent[User]] =
    collection.watch()

  val insertsOnly = WatchOptions[User](
    pipeline = Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("insert")))))
  )

  def inserts(collection: MongoCollection[IO, S, User]): S[ChangeEvent[User]] =
    collection.watch(insertsOnly)

  def resumed(collection: MongoCollection[IO, S, User], token: BsonDocument): S[ChangeEvent[User]] =
    collection.watch(
      WatchOptions
        .resumeAfter[User](token)
        .withFullDocument(FullDocument.DEFAULT)
        .withMaxAwaitTime(2.seconds)
        .withBatchSize(64)
    )

  def startedAfter(collection: MongoCollection[IO, S, User], token: BsonDocument): S[ChangeEvent[User]] =
    collection.watch(WatchOptions.default[User].startingAfter(token))

  def startedAt(collection: MongoCollection[IO, S, User], timestamp: BsonTimestamp): S[ChangeEvent[User]] =
    collection.watch(WatchOptions.default[User].startingAt(timestamp))

  def survivingBadDocuments(collection: MongoCollection[IO, S, User]): S[DecodeResult[ChangeEvent[User]]] =
    collection.watchAttempting()

  def databaseWide(db: MongoDatabase[IO, S]): S[ChangeEvent[User]] = db.watchAs[User]()

  // --- WithId, for entities that do not carry their own id ---

  final case class Note(text: String) derives WireCodec

  object Note:
    given BsonDocumentCodec[Note] = DocumentCodecBridge.toDocumentCodec[Note]

  def objectIdRepository(db: MongoDatabase[IO, S]): IO[Option[WithId[ObjectId, Note]]] =
    val id = ObjectId.get()

    for
      notes <- BaseMongoRepository.objectId[IO, S, Note](db, "notes")
      _     <- notes.insertOne(WithId(id, Note("remember this")))
      found <- notes.findOne(id)
    yield found

  // --- Derivation config ---

  object snakeCased:
    given WireCodecConfig = WireCodecConfig.SnakeCase.withDiscriminatorNaming(FieldNaming.snakeCase)

    final case class Person(firstName: String, lastName: String) derives WireCodec

  // --- Absent Option fields ---

  final case class Contact(name: String, email: Option[String]) derives WireCodec

  val omitted: Contact = Contact("bob", None)

  object nullsKept:
    given WireCodecConfig = WireCodecConfig.Default.withOmitNoneFields(false)

    final case class LegacyContact(name: String, email: Option[String]) derives WireCodec

  // --- Bridge configuration ---

  object tuned:
    given RsBridgeConfig = RsBridgeConfig(bufferSize = 512, timeout = Some(5.seconds), strictSingleResult = true)

    def client: IO[MongoClient[IO, S]] = MongoClient.fromConnectionString[IO, S]("mongodb://localhost:27017")

  // --- Update results as a decision ---

  def report(result: UpdateResult): String =
    if result.wasUpserted then s"inserted ${result.upsertedId}"
    else if result.wasApplied then s"matched ${result.matchedCount}, changed ${result.modifiedCount}"
    else "nothing matched"
