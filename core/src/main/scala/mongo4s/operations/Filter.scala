package mongo4s.operations

import scala.annotation.targetName

import org.bson.{BsonArray, BsonBoolean, BsonDocument, BsonRegularExpression, BsonValue}

import mongo4s.FieldPath
import mongo4s.bson.FieldNaming

import scala.jdk.CollectionConverters.given

enum Filter[E]:
  case Eq[T](path: FieldPath, value: BsonValue)                    extends Filter[T]
  case Ne[T](path: FieldPath, value: BsonValue)                    extends Filter[T]
  case Gt[T](path: FieldPath, value: BsonValue)                    extends Filter[T]
  case Gte[T](path: FieldPath, value: BsonValue)                   extends Filter[T]
  case Lt[T](path: FieldPath, value: BsonValue)                    extends Filter[T]
  case Lte[T](path: FieldPath, value: BsonValue)                   extends Filter[T]
  case In[T](path: FieldPath, values: List[BsonValue])             extends Filter[T]
  case Nin[T](path: FieldPath, values: List[BsonValue])            extends Filter[T]
  case Exists[T](path: FieldPath, exists: Boolean)                 extends Filter[T]
  case Regex[T](path: FieldPath, pattern: String, options: String) extends Filter[T]
  case And[T](filters: List[Filter[T]])                            extends Filter[T]
  case Or[T](filters: List[Filter[T]])                             extends Filter[T]
  case Not[T](filter: Filter[T])                                   extends Filter[T]
  case MatchAll[T]()                                               extends Filter[T]
  case MatchNone[T]()                                              extends Filter[T]
  case Raw[T](document: BsonDocument)                              extends Filter[T]

  @targetName("and")
  infix def &&(other: Filter[E]): Filter[E] = Filter.and(this, other)

  @targetName("or")
  infix def ||(other: Filter[E]): Filter[E] = Filter.or(this, other)

  @targetName("not")
  def unary_! : Filter[E] = Not(this)

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Eq(path, value)               => BsonDocument(path.render(naming), value)
    case Ne(path, value)               => Filter.operator(naming, path, "$ne", value)
    case Gt(path, value)               => Filter.operator(naming, path, "$gt", value)
    case Gte(path, value)              => Filter.operator(naming, path, "$gte", value)
    case Lt(path, value)               => Filter.operator(naming, path, "$lt", value)
    case Lte(path, value)              => Filter.operator(naming, path, "$lte", value)
    case In(path, values)              => Filter.operator(naming, path, "$in", BsonArray(values.asJava))
    case Nin(path, values)             => Filter.operator(naming, path, "$nin", BsonArray(values.asJava))
    case Exists(path, exists)          => Filter.operator(naming, path, "$exists", BsonBoolean(exists))
    case Regex(path, pattern, options) => BsonDocument(path.render(naming), BsonRegularExpression(pattern, options))
    case And(filters)                  => BsonDocument("$and", BsonArray(filters.map(_.toBson(naming)).asJava))
    case Or(filters)                   => BsonDocument("$or", BsonArray(filters.map(_.toBson(naming)).asJava))
    case Not(filter)                   => BsonDocument("$nor", BsonArray(List(filter.toBson(naming)).asJava))
    case MatchAll()                    => BsonDocument()
    case MatchNone()                   => BsonDocument("$nor", BsonArray(List(BsonDocument()).asJava))
    case Raw(document)                 => document

object Filter:
  def all[E]: Filter[E]  = MatchAll()
  def none[E]: Filter[E] = MatchNone()

  def and[E](filters: Filter[E]*): Filter[E] =
    val relevant = filters.toList.filterNot(isAll)
    if relevant.exists(isNone) then MatchNone()
    else
      relevant match
        case Nil           => MatchAll()
        case single :: Nil => single
        case many          => And(many)

  def or[E](filters: Filter[E]*): Filter[E] =
    val relevant = filters.toList.filterNot(isNone)
    if relevant.exists(isAll) then MatchAll()
    else
      relevant match
        case Nil           => MatchNone()
        case single :: Nil => single
        case many          => Or(many)

  private def isAll[E](filter: Filter[E]): Boolean = filter match
    case MatchAll() => true
    case _          => false

  private def isNone[E](filter: Filter[E]): Boolean = filter match
    case MatchNone() => true
    case _           => false

  private def operator(naming: FieldNaming, path: FieldPath, name: String, value: BsonValue): BsonDocument =
    BsonDocument(path.render(naming), BsonDocument(name, value))
