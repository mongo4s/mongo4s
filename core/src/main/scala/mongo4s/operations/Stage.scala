package mongo4s.operations

import org.bson.*

import mongo4s.{Field, FieldPath}
import mongo4s.bson.{BsonEncoder, FieldNaming}

import scala.jdk.CollectionConverters.given

enum Stage[E](val key: String):
  case MatchStage(filter: Filter[E])                                extends Stage[E]("$match")
  case ProjectStage(projection: Projection[E])                      extends Stage[E]("$project")
  case SortStage(sort: Sort[E])                                     extends Stage[E]("$sort")
  case Limit(n: Int)                                                extends Stage[E]("$limit")
  case Skip(n: Int)                                                 extends Stage[E]("$skip")
  case Count(fieldName: String)                                     extends Stage[E]("$count")
  case Unwind(path: FieldPath, preserveNullAndEmptyArrays: Boolean) extends Stage[E]("$unwind")

  case Lookup(
      from: String,
      localField: FieldPath,
      foreignField: FieldPath,
      as: String,
  ) extends Stage[E]("$lookup")

  case LookupPipeline[T, B](
      from: String,
      let: Option[BsonDocument],
      pipeline: List[Stage[B]],
      as: String,
  ) extends Stage[T]("$lookup")

  case GraphLookup[T, B](
      from: String,
      startWith: BsonValue,
      connectFrom: FieldPath,
      connectTo: FieldPath,
      as: String,
      options: GraphLookupOptions[B],
  ) extends Stage[T]("$graphLookup")

  case Group(by: Option[FieldPath], accumulators: List[(String, Accumulator[E])]) extends Stage[E]("$group")
  case AddFields(fields: List[(String, BsonValue)])                               extends Stage[E]("$addFields")
  case ReplaceRoot(path: FieldPath)                                               extends Stage[E]("$replaceRoot")
  case Sample(size: Int)                                                          extends Stage[E]("$sample")
  case UnionWith(collection: String)                                              extends Stage[E]("$unionWith")
  case Facet(facets: List[(String, List[Stage[E]])])                              extends Stage[E]("$facet")

  case Bucket(
      groupBy: FieldPath,
      boundaries: List[BsonValue],
      default: Option[BsonValue],
      output: List[(String, Accumulator[E])],
  ) extends Stage[E]("$bucket")

  case Densify(
      path: FieldPath,
      partitionBy: List[FieldPath],
      range: DensifyRange,
  ) extends Stage[E]("$densify")

  case SetWindowFields(
      partitionBy: Option[FieldPath],
      sortBy: Sort[E],
      output: List[(String, WindowOutput[E])],
  ) extends Stage[E]("$setWindowFields")

  case Out(collection: String, options: OutOptions)     extends Stage[E]("$out")
  case Merge(collection: String, options: MergeOptions) extends Stage[E]("$merge")

  case Raw(document: BsonDocument) extends Stage[E]("")

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Stage.MatchStage(filter)                         => BsonDocument(key, filter.toBson(naming))
    case Stage.ProjectStage(projection)                   => BsonDocument(key, projection.toBson(naming))
    case Stage.SortStage(sort)                            => BsonDocument(key, sort.toBson(naming))
    case Stage.Limit(n)                                   => BsonDocument(key, BsonInt32(n))
    case Stage.Skip(n)                                    => BsonDocument(key, BsonInt32(n))
    case Stage.Count(fieldName)                           => BsonDocument(key, BsonString(fieldName))
    case Stage.Unwind(path, preserveNullAndEmptyArrays)   =>
      BsonDocument(
        key,
        BsonDocument("path", BsonString("$" + path.render(naming)))
          .append("preserveNullAndEmptyArrays", BsonBoolean(preserveNullAndEmptyArrays)),
      )
    case Stage.Lookup(from, localField, foreignField, as) =>
      BsonDocument(
        key,
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

      BsonDocument(key, lookup)

    case Stage.GraphLookup(from, startWith, connectFrom, connectTo, as, options) =>
      val graph = BsonDocument("from", BsonString(from))
        .append("startWith", startWith)
        .append("connectFromField", BsonString(connectFrom.render(naming)))
        .append("connectToField", BsonString(connectTo.render(naming)))
        .append("as", BsonString(as))

      options.maxDepth.foreach(value => graph.append("maxDepth", BsonInt32(value)): Unit)
      options.depthField.foreach(value => graph.append("depthField", BsonString(value)): Unit)
      options.restrictSearch.foreach(filter => graph.append("restrictSearchWithMatch", filter.toBson(naming)): Unit)

      BsonDocument(key, graph)

    case Stage.Group(by, accumulators) =>
      val group = BsonDocument(
        FieldPath.IdName,
        by.fold(BsonNull.VALUE: BsonValue)(path => BsonString("$" + path.render(naming)))
      )
      accumulators.foreach((name, accumulator) => group.append(name, accumulator.toBson(naming)))
      BsonDocument(key, group)

    case Stage.AddFields(fields) =>
      BsonDocument(
        key,
        fields.foldLeft(BsonDocument())((acc, entry) => acc.append(entry._1, entry._2))
      )

    case Stage.ReplaceRoot(path)     => BsonDocument(key, BsonDocument("newRoot", BsonString("$" + path.render(naming))))
    case Stage.Sample(size)          => BsonDocument(key, BsonDocument("size", BsonInt32(size)))
    case Stage.UnionWith(collection) => BsonDocument(key, BsonString(collection))

    case Stage.Facet(facets) =>
      val document = BsonDocument()
      facets.foreach { (name, stages) =>
        document.append(name, BsonArray(stages.map(_.toBson(naming)).asJava))
      }
      BsonDocument(key, document)

    case Stage.Bucket(groupBy, boundaries, default, output) =>
      val bucket = BsonDocument("groupBy", BsonString("$" + groupBy.render(naming)))
        .append("boundaries", BsonArray(boundaries.asJava))

      default.foreach(value => bucket.append("default", value))

      if output.nonEmpty
      then
        val fields = BsonDocument()
        output.foreach((name, accumulator) => fields.append(name, accumulator.toBson(naming)))
        bucket.append("output", fields): Unit

      BsonDocument(key, bucket)

    case Stage.Densify(path, partitionBy, range) =>
      val densify = BsonDocument("field", BsonString(path.render(naming)))

      if partitionBy.nonEmpty
      then densify.append("partitionByFields", BsonArray(partitionBy.map(p => BsonString(p.render(naming)): BsonValue).asJava)): Unit

      densify.append("range", range.toBson)

      BsonDocument(key, densify)

    case Stage.SetWindowFields(partitionBy, sortBy, output) =>
      val windowFields = BsonDocument()

      partitionBy.foreach(path => windowFields.append("partitionBy", BsonString("$" + path.render(naming))))

      if !sortBy.isEmpty
      then windowFields.append("sortBy", sortBy.toBson(naming)): Unit

      val fields = BsonDocument()
      output.foreach { (name, windowed) =>
        val field = windowed.accumulator.toBson(naming)
        windowed.window.foreach(window => field.append("window", window.toBson))
        fields.append(name, field)
      }

      windowFields.append("output", fields)

      BsonDocument(key, windowFields)

    case Stage.Out(collection, options)   => BsonDocument(key, Stage.outTarget(collection, options))
    case Stage.Merge(collection, options) => BsonDocument(key, Stage.mergeTarget(collection, options))
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

  def bucketBy[E, A](field: Field[E, A], boundaries: Seq[A], default: Option[BsonValue] = None)(
      output: (String, Accumulator[E])*
  )(using encoder: BsonEncoder[A]): Stage[E] =
    require(boundaries.size >= 2, s"$$bucket needs at least two boundaries to make one bucket, got ${boundaries.size}")

    Bucket(field.path, boundaries.toList.map(encoder.encode), default, output.toList)
  end bucketBy

  def densify[E, A](field: Field[E, A], range: DensifyRange, partitionBy: Seq[FieldPath] = Seq.empty): Stage[E] =
    Densify(field.path, partitionBy.toList, range)

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
