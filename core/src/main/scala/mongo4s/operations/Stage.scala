package mongo4s.operations

import org.bson.*

import mongo4s.bson.FieldNaming
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
  case Group(by: Option[FieldPath], accumulators: List[(String, Accumulator[E])])
  case AddFields(fields: List[(String, BsonValue)])
  case ReplaceRoot(path: FieldPath)
  case Sample(size: Int)
  case UnionWith(collection: String)
  case Facet(facets: List[(String, List[Stage[E]])])
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
    case Stage.Group(by, accumulators)                    =>
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
