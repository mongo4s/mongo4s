package mongo4s.examples

import cats.effect.IO
import org.bson.types.ObjectId
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString, BsonTimestamp}
import com.mongodb.{ExplainVerbosity, WriteConcern}
import com.mongodb.client.model.{Collation, TimeSeriesGranularity}
import com.mongodb.client.model.changestream.FullDocument

import mongo4s.cats.{CatsStream, MongoClientResource}
import mongo4s.results.{BulkWriteResult, ExplainSummary, UpdateResult}
import mongo4s.changestream.{ChangeEvent, WatchOptions}
import mongo4s.repositories.{BaseMongoRepository, Page}
import mongo4s.bson.{BsonDocumentCodec, BsonTypeName, DecodeResult, FieldNaming}
import mongo4s.bson.direct.{DocumentCodecBridge, WireCodec, WireCodecConfig}
import mongo4s.operations.*
import mongo4s.operations.geometry.Geometry
import mongo4s.{Field, MongoClient, MongoCollection, MongoDatabase, MongoError, PrimaryKey, RsBridgeConfig, WithId, withTransaction}

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
    given PrimaryKey[Order, (userId: String, seq: Int)] =
      PrimaryKey.compound(o => (userId = o.userId, seq = o.seq), FieldNaming.snakeCase)

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
  val isWhole  = ageField.hasType(BsonTypeName.Int) && ageField.mod(2, 0)

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

  // $each modifiers keep the array capped and ordered on the server rather than in the application.
  val recentTags = tagsField.pushAll(List("vip"), PushOptions.default[String].sortedAscending.withSlice(10))
  val combined   = Update.combine(setAge, tagged)

  // Raw carries operators the AST does not model, and merges with the typed ones rather than replacing them.
  val withBit = Update
    .set(Field.of[User, String](_.name), "bob")
    .and(
      Update.Raw[User](BsonDocument("$unset", BsonDocument("legacyNickname", BsonString(""))))
    )

  // A pipeline computes from the document's own values, which operators cannot.
  val exclaimed = Update.pipeline(
    Stage.addFields[User]("name" := Field.of[User, String](_.name).expr ++ Expr.lit("!"))
  )

  val scoreDropped = Update.pipeline(Stage.unset(Field.of[User, Option[Long]](_.score)))

  // --- Array elements ---

  val quantity = Field.of[Item, Int](_.quantity)

  def resetEveryQuantity(collection: MongoCollection[IO, S, Order]): IO[UpdateResult] =
    collection.updateOne(Field.of[Order, Int](_.seq).equalTo(1), Update.set(itemsField.each / quantity, 0))

  def bumpMatched(collection: MongoCollection[IO, S, Order]): IO[UpdateResult] =
    collection.updateOne(itemsField.elemMatch(Field.of[Item, String](_.sku).equalTo("abc")), Update.inc(itemsField.matched / quantity, 1))

  def raiseLowQuantities(collection: MongoCollection[IO, S, Order]): IO[UpdateResult] =
    val low = Field.element[Item]("low")

    collection.updateOne(
      Field.of[Order, Int](_.seq).equalTo(1),
      Update.set(itemsField.where(low) / quantity, 100),
      UpdateOptions.default.withArrayFilters(Seq((low / quantity).lt(3))),
    )

  val emptied   = itemsField.pullWhere(quantity.lt(1))
  val inRange   = tagsField.elemMatch(Field.self[String].gte("a") && Field.self[String].lt("n"))
  val anyNumber = ageField.hasAnyType(BsonTypeName.Int, BsonTypeName.Long)
  val synced    = Update.currentTimestamp(Field.stored[User, BsonTimestamp]("syncedAt"))

  // --- Concerns ---

  def durableWrite(collection: MongoCollection[IO, S, User], user: User, other: User): IO[Unit] =
    val durable = collection.withWriteConcern(WriteConcern.MAJORITY)
    durable.insertOne(user) *> collection.insertOne(other).void

  // --- Results ---

  def upsertedId(collection: MongoCollection[IO, S, User]): IO[Option[org.bson.BsonValue]] =
    collection.updateOne(named, setAge, UpdateOptions.upsert).map(r => if r.wasUpserted then r.upsertedId else None)

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

  def snapshotRead(client: MongoClient[IO, S], users: MongoCollection[IO, S, User]): IO[(Long, List[User])] =
    client.withSession(
      for
        before <- users.count()
        rows   <- users.find().all
      yield (before, rows),
      SessionOptions.snapshot,
    )

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

  val trimmed = Seq(Stage.unset(ageField).and(Field.of[User, List[String]](_.tags)))

  val nearest = Seq(
    Stage.geoNear(
      Field.stored[User, Geometry.Point]("location"),
      Geometry.Point(13.405, 52.52),
      "km",
      GeoNearOptions.default[User].where(adults).withMaxDistance(5000).withDistanceMultiplier(0.001),
    )
  )

  val spread  = Seq(Stage.bucketAuto(Field.of[User, String](_.name), 4)("count" -> Accumulator.count[User]))
  val rounded = Seq(Stage.bucketAutoRounded(ageField, 5, BucketGranularity.R10)())
  val popular = Seq(Stage.sortByCount(Field.of[User, String](_.name)))

  private val nameField = Field.of[User, String](_.name)

  val ranked = Seq(
    Stage.groupBy(Field.of[User, List[String]](_.tags))(
      "best"   -> Accumulator.topN(Sort.desc(scoreField), nameField, 3),
      "eldest" -> Accumulator.top(Sort.desc(ageField), nameField),
      "recent" -> Accumulator.lastN(nameField, 2),
      "oldest" -> Accumulator.maxN(ageField, 2),
      "median" -> Accumulator.median(ageField),
      "p90"    -> Accumulator.percentile(ageField, 0.9),
      "spread" -> Accumulator.stdDevPop(ageField),
    )
  )

  val windowed = Seq(
    Stage.setWindowFields(Sort.asc(ageField))(
      "total" -> WindowOutput(Accumulator.sum(ageField)),
      "place" -> WindowFunction.rank[User],
      "prev"  -> WindowFunction.shift(ageField, by = -1, default = Some(0)),
      "kept"  -> WindowFunction.locf(scoreField),
      "area"  -> WindowFunction.integral(ageField, Window.documents(WindowBound.Unbounded, WindowBound.Current)),
      "trend" -> WindowFunction.expMovingAvg(ageField, 7),
    )
  )

  val filled = Seq(Stage.fill(Sort.asc(ageField))(FillOutput.linear(scoreField), FillOutput.value(nameField, "none")))

  val pruned = Seq(Stage.redact(Expr.cond(ageField.expr === Expr.lit(1), Expr.prune, Expr.descend)))

  val adultOwner = Stage.lookupWithKeys[Order, User, String](
    from = "users",
    localField = Field.of[Order, String](_.userId),
    foreignField = Field.of[User, String](_.id),
    pipeline = List(Stage.matching(adults)),
    as = "adultOwner",
  )

  val flagged    = scoreField.bitsAllSet(4) && Filter.jsonSchema[User](BsonDocument("required", BsonArray(java.util.List.of(BsonString("age")))))
  val flagRaised = scoreField.bitOr(4L)
  val ageMasked  = Update.bitAnd(ageField, 6)

  def aggregated(collection: MongoCollection[IO, S, User]): IO[List[User]] =
    collection.aggregate[User](pipeline).allowDiskUse(true).maxTime(10.seconds).all

  // --- Indexes ---

  def indexes(collection: MongoCollection[IO, S, User]): IO[List[BsonDocument]] =
    for
      _      <- collection.createIndex(Index.ascending(Field.of[User, String](_.name)).descending(ageField).named("name_age"))
      _      <- collection.createIndex(Index.unique(Field.of[User, String](_.id)))
      _      <- collection.createIndex(Index.ascending(ageField).expiringAfter(30.days))
      _      <- collection.createIndex(Index.ascending(Field.stored[User, java.time.Instant]("expiresAt")).expiringAtFieldTime)
      _      <- collection.createIndex(Index.ascending(ageField).where(adults))
      _      <- collection.createIndex(Index.empty[User].text(Field.of[User, String](_.name)).withSparse)
      _      <- collection.createIndex(Index.hashed(Field.of[User, String](_.id)))
      _      <- collection.createIndex(Index.geo2DSphere(Field.stored[User, Any]("location")))
      _      <- collection.createIndex(Index.ascending(ageField).withHidden)
      _      <- collection.createIndex(Index.ascending(Field.stored[User, Any]("$**")))
      listed <- collection.listIndexes
      _      <- collection.dropIndex("name_age")
      _      <- collection.createIndexes(List(Index.ascending(Field.of[User, String](_.name)), Index.descending(ageField)))
      _      <- collection.dropIndexes
    yield listed

  // --- Keyset paging ---

  def walk(users: BaseMongoRepository[IO, S, User, String]): IO[List[User]] =
    def from(after: Option[String]): IO[List[User]] =
      users.findPage(100, after).flatMap { page =>
        page.next.fold(IO.pure(page.items))(next => from(Some(next)).map(page.items ++ _))
      }

    from(None)
  end walk

  def newestFirst(users: BaseMongoRepository[IO, S, User, String]): IO[List[User]] =
    users.findPage(20, descending = true).map(_.items)

  // --- AST-free aggregation ---

  final case class ByAge(_id: Int, total: Int) derives WireCodec

  def byAge(collection: MongoCollection[IO, S, User]): IO[List[ByAge]] =
    collection.aggregateDirect[ByAge](Seq(Stage.groupBy(ageField)("total" -> Accumulator.count[User]))).all

  // --- Partial reads ---

  def summaries(collection: MongoCollection[IO, S, User]): IO[List[(name: String, age: Int)]] =
    collection.find(adults).sort(Sort.asc(Field.of[User, String](_.name))).selectAs[(name: String, age: Int)].all

  def summaryNames(collection: MongoCollection[IO, S, User]): IO[List[String]] =
    summaries(collection).map(_.map(_.name))

  // --- Creating a collection ---

  def createCollections(database: MongoDatabase[IO, S], jsonSchema: BsonDocument): IO[Unit] =
    for
      _ <- database.createCollection("events", CreateCollectionOptions.capped(8 * 1024 * 1024).withMaxDocuments(10000))
      _ <- database.createCollection(
             "readings",
             CreateCollectionOptions.default
               .withTimeSeries(TimeSeries.on("recordedAt").withMetaField("sensor").withGranularity(TimeSeriesGranularity.MINUTES))
               .expiringAfter(30.days),
           )
      _ <- database.createCollection("people", CreateCollectionOptions.default.withValidator(jsonSchema))
      _ <- database.createCollection("orders", CreateCollectionOptions.default.withClusteredIndex(ClusteredIndex.onId))
    yield ()

  def archive(collection: MongoCollection[IO, S, User]): IO[Unit] = collection.renameCollection("people_archive")

  def archiveOver(collection: MongoCollection[IO, S, User]): IO[Unit] = collection.renameCollection("people_archive", dropTarget = true)

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
    users.getByFilter(adults, Page.first(100))

  def atomically(users: BaseMongoRepository[IO, S, User, String]): IO[Option[User]] =
    users.findOneAndUpdate("1", birthday)

  // --- Change streams ---

  def allEvents(collection: MongoCollection[IO, S, User]): S[ChangeEvent[User]] =
    collection.watch()

  val insertsOnly = WatchOptions
    .default[User]
    .withPipeline(Seq(Stage.raw(BsonDocument("$match", BsonDocument("operationType", BsonString("insert"))))))

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

  def survivingBadDocuments(collection: MongoCollection[IO, S, User]): S[ChangeEvent[DecodeResult[User]]] =
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
    given RsBridgeConfig = RsBridgeConfig.default.withBufferSize(512).withTimeout(5.seconds).withStrictSingleResult

    def client: IO[MongoClient[IO, S]] = MongoClient.fromConnectionString[IO, S]("mongodb://localhost:27017")

  // --- Update results as a decision ---

  object slicing:
    final case class Post(title: String, comments: List[String])

    val titleField: Field[Post, String]          = Field.of[Post, String](_.title)
    val commentsField: Field[Post, List[String]] = Field.of[Post, List[String]](_.comments)

    val lastFive: Projection[Post]  = Projection.empty[Post].slice(commentsField, -5)
    val aWindow: Projection[Post]   = Projection.empty[Post].sliceFrom(commentsField, skip = 20, count = 10)
    val withTitle: Projection[Post] = Projection.empty[Post].include(titleField).slice(commentsField, 5)

  def ranked(collection: MongoCollection[IO, S, User]): IO[List[User]] =
    collection.find(Filter.text("mongodb")).sort(Sort.byTextScore()).all

  def queryPlan(collection: MongoCollection[IO, S, User], adults: Filter[User], nameField: Field[User, String]): IO[BsonDocument] =
    collection.find(adults).sort(Sort.asc(nameField)).explain()

  def planSummary(collection: MongoCollection[IO, S, User], adults: Filter[User]): IO[ExplainSummary] =
    collection.find(adults).explain(ExplainVerbosity.EXECUTION_STATS).map(ExplainSummary.of)

  def measuredPlan(collection: MongoCollection[IO, S, User], adults: Filter[User]): IO[BsonDocument] =
    collection.find(adults).explain(ExplainVerbosity.EXECUTION_STATS)

  def insertOrReplace(
      collection: MongoCollection[IO, S, User],
      user: User,
      byEmail: Filter[User],
      retry: IO[Unit],
  ): IO[Unit] =
    collection.insertOne(user).void.recoverWith {
      case MongoError.DuplicateKey(_)  => collection.replaceOne(byEmail, user).void
      case MongoError.WriteConflict(_) => retry
    }

  def report(result: UpdateResult): String =
    if result.wasUpserted then s"inserted ${result.upsertedId}"
    else if result.wasApplied then s"matched ${result.matchedCount}, changed ${result.modifiedCount}"
    else "nothing matched"

  // --- Write options ---

  def oldestRenamed(collection: MongoCollection[IO, S, User]): IO[UpdateResult] =
    collection.updateOne(Filter.all, Update.set(nameField, "oldest"), UpdateOptions.default.withSort(Sort.asc(ageField)))

  def importUsers(collection: MongoCollection[IO, S, User], users: List[User]): IO[Unit] =
    collection.insertMany(users, InsertManyOptions.unordered).void

  def bulkUnordered(collection: MongoCollection[IO, S, User]): IO[BulkWriteResult] =
    collection.bulkWrite(
      Seq(WriteCommand.deleteOne(ageField.lt(0), DeleteOptions.default.withComment("cleanup"))),
      BulkWriteOptions.unordered.withComment("nightly"),
    )

  def acrossCollections(client: MongoClient[IO, S], users: MongoCollection[IO, S, User], orders: MongoCollection[IO, S, Order]) =
    client.bulkWrite(
      List(
        users.write(WriteCommand.InsertOne(User("3", "Cleo", 28, Nil, None))),
        orders.write(WriteCommand.updateOne(Field.of[Order, Int](_.seq).equalTo(1), Update.inc(Field.of[Order, Map[String, Int]](_.totals).at("EUR"), 5))),
      )
    )

  // --- Tailable cursors ---

  def tail(collection: MongoCollection[IO, S, User]): S[User] =
    collection.find().cursorType(com.mongodb.CursorType.TailableAwait).maxAwaitTime(1.second).stream

  // --- Aggregation expressions ---

  final case class Line(price: Double, qty: Int, discount: Option[Double], createdAt: java.time.Instant, city: String) derives WireCodec

  private val price     = Field.of[Line, Double](_.price)
  private val qty       = Field.of[Line, Int](_.qty)
  private val discount  = Field.of[Line, Option[Double]](_.discount)
  private val createdAt = Field.of[Line, java.time.Instant](_.createdAt)
  private val lineCity  = Field.of[Line, String](_.city)

  val total: Expr[Line, Double] = price.expr * qty.expr
  val net                       = total - discount.expr.orElse(Expr.lit(0.0))
  val size                      = Expr.cond(qty.expr > Expr.lit(10), Expr.lit("bulk"), Expr.lit("single"))
  val sized                     = Expr.switch((qty.expr > Expr.lit(100)) -> Expr.lit("huge"), (qty.expr > Expr.lit(10)) -> Expr.lit("big"))(Expr.lit("small"))

  val computedLines = Seq(
    Stage.addFields[Line]("total"                                                             := total, "size" := size),
    Stage.groupByExpr(createdAt.expr.year)("revenue"                                          -> Accumulator.sum(price.expr * qty.expr)),
    Stage.groupByKeys[Line]("city" := lineCity.expr, "month" := createdAt.expr.month)("lines" -> Accumulator.count[Line]),
    Stage.replaceWith(Expr.mergeObjects(Expr.root[Line], Expr.document("total" := total))),
  )

  val expensive = Filter.expr(total > Expr.lit(100.0))
  val initials  = Projection.empty[User].include(nameField).compute("initial" := nameField.expr.substring(0, 1))

  val manyItems = itemsField.expr.filter(item => item.get(quantity) > Expr.lit(1))
  val itemSizes = itemsField.expr.map(item => item.get(quantity) * Expr.lit(2))

  // --- Atlas Search ---

  final case class Movie(title: String, plot: String, year: Int, embedding: List[Double]) derives WireCodec

  private val title     = Field.of[Movie, String](_.title)
  private val plot      = Field.of[Movie, String](_.plot)
  private val year      = Field.of[Movie, Int](_.year)
  private val embedding = Field.of[Movie, List[Double]](_.embedding)

  def searchIndexes(collection: MongoCollection[IO, S, Movie]): IO[List[String]] =
    collection.createSearchIndexes(
      List(
        SearchIndex.dynamic[Movie](),
        SearchIndex.vector("plots", embedding, 1536, VectorSimilarity.Cosine).filteringOn(year),
      )
    )

  val matrix = SearchOperator.compound[Movie](
    must = List(SearchOperator.text(plot, "hacker").orIn(title).fuzzy(1)),
    filter = List(SearchOperator.atLeast(year, 1999)),
  )

  val searched = Seq(
    Stage.search(matrix, SearchOptions.default.withCount(SearchCount.total)),
    Stage.project(Projection.empty[Movie].include(title).compute("score" := Expr.searchScore)),
  )

  def nearest(queryVector: List[Double]) =
    Stage.vectorSearch("plots", embedding, queryVector, limit = 10, numCandidates = 200, filter = Some(year.gte(2000)))

  def weighted(collection: MongoCollection[IO, S, Movie]): IO[String] =
    collection.createIndex(Index.text(title).text(plot).withWeight(title, 10).withDefaultLanguage("spanish"))
