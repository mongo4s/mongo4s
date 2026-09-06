package mongo4s.operations

import org.bson.*

import mongo4s.bson.{BsonEncoder, FieldNaming}
import mongo4s.{Field, FieldPath}

import scala.jdk.CollectionConverters.given

enum Stage[E]:
  case MatchStage(filter: Filter[E])
  case ProjectStage(projection: Projection[E])
  case SortStage(sort: Sort[E])
  case Limit(n: Int)
  case Skip(n: Int)
  case Count(fieldName: String)
  case Unwind(path: FieldPath, preserveNullAndEmptyArrays: Boolean)
  case Lookup(from: String, localField: FieldPath, foreignField: FieldPath, as: String)
  case LookupPipeline[T, B](from: String, let: Option[BsonDocument], pipeline: List[Stage[B]], as: String) extends Stage[T]

  case GraphLookup[T, B](
      from: String,
      startWith: BsonValue,
      connectFrom: FieldPath,
      connectTo: FieldPath,
      as: String,
      options: GraphLookupOptions[B],
  ) extends Stage[T]
  case Group(by: Option[FieldPath], accumulators: List[(String, Accumulator[E])])
  case AddFields(fields: List[(String, BsonValue)])
  case ReplaceRoot(path: FieldPath)
  case Sample(size: Int)
  case UnionWith(collection: String)
  case Facet(facets: List[(String, List[Stage[E]])])
  case Bucket(groupBy: FieldPath, boundaries: List[BsonValue], default: Option[BsonValue], output: List[(String, Accumulator[E])])
  case Densify(path: FieldPath, partitionBy: List[FieldPath], range: DensifyRange)
  case SetWindowFields(partitionBy: Option[FieldPath], sortBy: Sort[E], output: List[(String, WindowOutput[E])])

  case Out(collection: String, options: OutOptions)
  case Merge(collection: String, options: MergeOptions)

  case Raw(document: BsonDocument)

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Stage.MatchStage(filter)                         => BsonDocument("$match", filter.toBson(naming))
    case Stage.ProjectStage(projection)                   => BsonDocument("$project", projection.toBson(naming))
    case Stage.SortStage(sort)                            => BsonDocument("$sort", sort.toBson(naming))
    case Stage.Limit(n)                                   => BsonDocument("$limit", BsonInt32(n))
    case Stage.Skip(n)                                    => BsonDocument("$skip", BsonInt32(n))
    case Stage.Count(fieldName)                           => BsonDocument("$count", BsonString(fieldName))
    case Stage.Unwind(path, preserveNullAndEmptyArrays)   =>
      BsonDocument(
        "$unwind",
        BsonDocument("path", BsonString("$" + path.render(naming)))
          .append("preserveNullAndEmptyArrays", BsonBoolean(preserveNullAndEmptyArrays)),
      )
    case Stage.Lookup(from, localField, foreignField, as) =>
      BsonDocument(
        "$lookup",
        BsonDocument("from", BsonString(from))
          .append("localField", BsonString(localField.render(naming)))
          .append("foreignField", BsonString(foreignField.render(naming)))
          .append("as", BsonString(as)),
      )
    case Stage.LookupPipeline(from, let, pipeline, as)    =>
      val lookup = BsonDocument("from", BsonString(from))

      let.foreach(value => lookup.append("let", value): Unit)
      lookup.append("pipeline", BsonArray(pipeline.map(_.toBson(naming)).asJava)): Unit
      lookup.append("as", BsonString(as)): Unit

      BsonDocument("$lookup", lookup)

    case Stage.GraphLookup(from, startWith, connectFrom, connectTo, as, options) =>
      val graph = BsonDocument("from", BsonString(from))
        .append("startWith", startWith)
        .append("connectFromField", BsonString(connectFrom.render(naming)))
        .append("connectToField", BsonString(connectTo.render(naming)))
        .append("as", BsonString(as))

      options.maxDepth.foreach(value => graph.append("maxDepth", BsonInt32(value)): Unit)
      options.depthField.foreach(value => graph.append("depthField", BsonString(value)): Unit)
      options.restrictSearch.foreach(filter => graph.append("restrictSearchWithMatch", filter.toBson(naming)): Unit)

      BsonDocument("$graphLookup", graph)

    case Stage.Group(by, accumulators) =>
      val group = BsonDocument(
        "_id",
        by.fold(BsonNull.VALUE: BsonValue)(path => BsonString("$" + path.render(naming)))
      )
      accumulators.foreach((name, accumulator) => group.append(name, accumulator.toBson(naming)))
      BsonDocument("$group", group)

    case Stage.AddFields(fields) =>
      BsonDocument(
        "$addFields",
        fields.foldLeft(BsonDocument())((acc, entry) => acc.append(entry._1, entry._2))
      )

    case Stage.ReplaceRoot(path)     => BsonDocument("$replaceRoot", BsonDocument("newRoot", BsonString("$" + path.render(naming))))
    case Stage.Sample(size)          => BsonDocument("$sample", BsonDocument("size", BsonInt32(size)))
    case Stage.UnionWith(collection) => BsonDocument("$unionWith", BsonString(collection))

    case Stage.Facet(facets) =>
      val document = BsonDocument()
      facets.foreach { (name, stages) =>
        document.append(name, BsonArray(stages.map(_.toBson(naming)).asJava))
      }
      BsonDocument("$facet", document)

    case Stage.Bucket(groupBy, boundaries, default, output) =>
      val bucket = BsonDocument("groupBy", BsonString("$" + groupBy.render(naming)))
        .append("boundaries", BsonArray(boundaries.asJava))

      default.foreach(value => bucket.append("default", value))

      if output.nonEmpty
      then
        val fields = BsonDocument()
        output.foreach((name, accumulator) => fields.append(name, accumulator.toBson(naming)))
        bucket.append("output", fields)

      BsonDocument("$bucket", bucket)

    case Stage.Densify(path, partitionBy, range) =>
      val densify = BsonDocument("field", BsonString(path.render(naming)))

      if partitionBy.nonEmpty
      then densify.append("partitionByFields", BsonArray(partitionBy.map(p => BsonString(p.render(naming)): BsonValue).asJava))

      densify.append("range", range.toBson)

      BsonDocument("$densify", densify)

    case Stage.SetWindowFields(partitionBy, sortBy, output) =>
      val windowFields = BsonDocument()

      partitionBy.foreach(path => windowFields.append("partitionBy", BsonString("$" + path.render(naming))))

      if !sortBy.isEmpty
      then windowFields.append("sortBy", sortBy.toBson(naming))

      val fields = BsonDocument()
      output.foreach { (name, windowed) =>
        val field = windowed.accumulator.toBson(naming)
        windowed.window.foreach(window => field.append("window", window.toBson))
        fields.append(name, field)
      }

      windowFields.append("output", fields)

      BsonDocument("$setWindowFields", windowFields)

    case Stage.Out(collection, options)   => BsonDocument("$out", Stage.outTarget(collection, options))
    case Stage.Merge(collection, options) => BsonDocument("$merge", Stage.mergeTarget(collection, options))
    case Stage.Raw(document)              => document

object Stage:
  def matching[E](filter: Filter[E]): Stage[E]        = MatchStage(filter)
  def project[E](projection: Projection[E]): Stage[E] = ProjectStage(projection)
  def sortBy[E](sort: Sort[E]): Stage[E]              = SortStage(sort)
  def limit[E](n: Int): Stage[E]                      = Limit(n)
  def skip[E](n: Int): Stage[E]                       = Skip(n)
  def count[E](fieldName: String): Stage[E]           = Count(fieldName)

  def unwind[E, A](field: Field[E, A], preserveNullAndEmptyArrays: Boolean = false): Stage[E] =
    Unwind(field.path, preserveNullAndEmptyArrays)

  def lookupWith[E, B](from: String, pipeline: List[Stage[B]], as: String, let: Option[BsonDocument] = None): Stage[E] =
    LookupPipeline(from, let, pipeline, as)

  def graphLookup[E, B, A](
      from: String,
      startWith: BsonValue,
      connectFrom: Field[B, A],
      connectTo: Field[B, A],
      as: String,
      options: GraphLookupOptions[B] = GraphLookupOptions.default[B],
  ): Stage[E] =
    GraphLookup(from, startWith, connectFrom.path, connectTo.path, as, options)

  def lookup[E, A](from: String, localField: Field[E, A], foreignField: String, as: String): Stage[E] =
    Lookup(
      from = from,
      localField = localField.path,
      foreignField = FieldPath.literal(foreignField),
      as = as,
    )

  def groupBy[E, A](field: Field[E, A])(accumulators: (String, Accumulator[E])*): Stage[E] =
    Group(Some(field.path), accumulators.toList)

  def groupAll[E](accumulators: (String, Accumulator[E])*): Stage[E] = Group(None, accumulators.toList)

  def addFields[E](fields: (String, BsonValue)*): Stage[E] = AddFields(fields.toList)

  def replaceRoot[E, A](field: Field[E, A]): Stage[E] = ReplaceRoot(field.path)

  def sample[E](size: Int): Stage[E]             = Sample(size)
  def unionWith[E](collection: String): Stage[E] = UnionWith(collection)

  def facet[E](facets: (String, List[Stage[E]])*): Stage[E] = Facet(facets.toList)

  /** `$bucket`: the boundaries are values of the field being grouped, so they are typed as such rather than as raw BSON. `default` names the bucket for everything
    * outside them; without it the server rejects such a document.
    */
  def bucketBy[E, A](field: Field[E, A], boundaries: Seq[A], default: Option[BsonValue] = None)(
      output: (String, Accumulator[E])*
  )(using encoder: BsonEncoder[A]): Stage[E] =
    require(boundaries.size >= 2, s"$$bucket needs at least two boundaries to make one bucket, got ${boundaries.size}")

    Bucket(field.path, boundaries.toList.map(encoder.encode), default, output.toList)
  end bucketBy

  /** `$densify`: fills the gaps in a series so every step is present, whether or not a document was written for it.
    *
    * The partition fields are `FieldPath`s rather than `Field`s because they are a heterogeneous list — write `Seq(sensorField.path)` at the call site.
    */
  def densify[E, A](field: Field[E, A], range: DensifyRange, partitionBy: Seq[FieldPath] = Seq.empty): Stage[E] =
    Densify(field.path, partitionBy.toList, range)

  /** `$setWindowFields`: a running total, a moving average, a rank — an accumulator computed over a span around each document rather than over the whole group.
    *
    * `sortBy` is what gives the window a direction, so it is required rather than optional; `partitionBy` is a `FieldPath` because it is one field of an unknown type —
    * write `sensorField.path`.
    */
  def setWindowFields[E](sortBy: Sort[E], partitionBy: Option[FieldPath] = None)(
      output: (String, WindowOutput[E])*
  ): Stage[E] =
    require(output.nonEmpty, "$setWindowFields with no output field computes nothing")

    SetWindowFields(partitionBy, sortBy, output.toList)
  end setWindowFields

  def out[E](collection: String, options: OutOptions = OutOptions.default): Stage[E] = Out(collection, options)

  def merge[E](collection: String, options: MergeOptions = MergeOptions.default): Stage[E] = Merge(collection, options)

  private def outTarget(collection: String, options: OutOptions): BsonValue =
    options.database match
      case None           => BsonString(collection)
      case Some(database) => BsonDocument("db", BsonString(database)).append("coll", BsonString(collection))

  private def mergeTarget(collection: String, options: MergeOptions): BsonValue =
    if options.isEmpty
    then BsonString(collection)
    else
      val into = options.database match
        case None           => BsonString(collection): BsonValue
        case Some(database) => BsonDocument("db", BsonString(database)).append("coll", BsonString(collection))

      val document = BsonDocument("into", into)

      if options.on.sizeIs == 1 then document.append("on", BsonString(options.on.head)): Unit
      else if options.on.nonEmpty then document.append("on", BsonArray(options.on.map(BsonString.apply).asJava)): Unit

      options.whenMatched.foreach(value => document.append("whenMatched", BsonString(value.wireName)): Unit)
      options.whenNotMatched.foreach(value => document.append("whenNotMatched", BsonString(value.wireName)): Unit)
      options.let.foreach(value => document.append("let", value): Unit)

      document
  end mergeTarget

  def raw[E](document: BsonDocument): Stage[E] = Raw(document)
