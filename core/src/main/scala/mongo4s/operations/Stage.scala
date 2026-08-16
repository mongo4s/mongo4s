package mongo4s.operations

import org.bson.{BsonBoolean, BsonDocument, BsonInt32, BsonString}

import mongo4s.{Field, FieldPath}
import mongo4s.bson.FieldNaming

enum Stage[E]:
  case MatchStage(filter: Filter[E])
  case ProjectStage(projection: Projection[E])
  case SortStage(sort: Sort[E])
  case Limit(n: Int)
  case Skip(n: Int)
  case Count(fieldName: String)
  case Unwind(path: FieldPath, preserveNullAndEmptyArrays: Boolean)
  case Lookup(from: String, localField: FieldPath, foreignField: FieldPath, as: String)
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
    case Stage.Raw(document)                              => document

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
    Lookup(from, localField.path, FieldPath.of(foreignField), as)

  def raw[E](document: BsonDocument): Stage[E] = Raw(document)
