package mongo4s.operations

import scala.annotation.targetName
import org.bson.*
import mongo4s.FieldPath
import mongo4s.bson.{BsonTypeName, FieldNaming}
import mongo4s.operations.geometry.{GeoShape, Geometry}

import scala.jdk.CollectionConverters.given

enum Filter[E](val key: String):
  case Eq[T](path: FieldPath, value: BsonValue)                    extends Filter[T]("")
  case Ne[T](path: FieldPath, value: BsonValue)                    extends Filter[T]("$ne")
  case Gt[T](path: FieldPath, value: BsonValue)                    extends Filter[T]("$gt")
  case Gte[T](path: FieldPath, value: BsonValue)                   extends Filter[T]("$gte")
  case Lt[T](path: FieldPath, value: BsonValue)                    extends Filter[T]("$lt")
  case Lte[T](path: FieldPath, value: BsonValue)                   extends Filter[T]("$lte")
  case In[T](path: FieldPath, values: List[BsonValue])             extends Filter[T]("$in")
  case Nin[T](path: FieldPath, values: List[BsonValue])            extends Filter[T]("$nin")
  case Exists[T](path: FieldPath, exists: Boolean)                 extends Filter[T]("$exists")
  case Regex[T](path: FieldPath, pattern: String, options: String) extends Filter[T]("")
  case ElemMatch[T, U](path: FieldPath, filter: Filter[U])         extends Filter[T]("$elemMatch")
  case All[T](path: FieldPath, values: List[BsonValue])            extends Filter[T]("$all")
  case Size[T](path: FieldPath, size: Int)                         extends Filter[T]("$size")
  case Type[T](path: FieldPath, bsonTypes: List[BsonTypeName])     extends Filter[T]("$type")
  case Mod[T](path: FieldPath, divisor: Long, remainder: Long)     extends Filter[T]("$mod")
  case Text[T](search: String, language: Option[String])           extends Filter[T]("$text")

  case Near[T](
      path: FieldPath,
      geometry: Geometry,
      minDistance: Option[Double],
      maxDistance: Option[Double],
  ) extends Filter[T]("$near")

  case NearSphere[T](
      path: FieldPath,
      geometry: Geometry,
      minDistance: Option[Double],
      maxDistance: Option[Double],
  ) extends Filter[T]("$nearSphere")

  case GeoWithin[T](path: FieldPath, shape: GeoShape)        extends Filter[T]("$geoWithin")
  case GeoIntersects[T](path: FieldPath, geometry: Geometry) extends Filter[T]("$geoIntersects")
  case Expr[T](expression: BsonDocument)                     extends Filter[T]("$expr")
  case JsonSchema[T](schema: BsonDocument)                   extends Filter[T]("$jsonSchema")
  case BitsAllSet[T](path: FieldPath, mask: Long)            extends Filter[T]("$bitsAllSet")
  case BitsAnySet[T](path: FieldPath, mask: Long)            extends Filter[T]("$bitsAnySet")
  case BitsAllClear[T](path: FieldPath, mask: Long)          extends Filter[T]("$bitsAllClear")
  case BitsAnyClear[T](path: FieldPath, mask: Long)          extends Filter[T]("$bitsAnyClear")

  case And[T](filters: List[Filter[T]]) extends Filter[T]("$and")
  case Or[T](filters: List[Filter[T]])  extends Filter[T]("$or")
  case Not[T](filter: Filter[T])        extends Filter[T]("$nor")
  case MatchAll[T]()                    extends Filter[T]("")
  case MatchNone[T]()                   extends Filter[T]("$nor")
  case Raw[T](document: BsonDocument)   extends Filter[T]("")

  @targetName("and")
  infix def &&(other: Filter[E]): Filter[E] = Filter.and(this, other)

  @targetName("or")
  infix def ||(other: Filter[E]): Filter[E] = Filter.or(this, other)

  @targetName("not")
  def unary_! : Filter[E] = Not(this)

  def toBson(naming: FieldNaming): BsonDocument = this match
    case Eq(path, value)               => if path.isEmpty then BsonDocument("$eq", value) else BsonDocument(path.render(naming), value)
    case Ne(path, value)               => Filter.operator(naming, path, key, value)
    case Gt(path, value)               => Filter.operator(naming, path, key, value)
    case Gte(path, value)              => Filter.operator(naming, path, key, value)
    case Lt(path, value)               => Filter.operator(naming, path, key, value)
    case Lte(path, value)              => Filter.operator(naming, path, key, value)
    case In(path, values)              => Filter.operator(naming, path, key, BsonArray(values.asJava))
    case Nin(path, values)             => Filter.operator(naming, path, key, BsonArray(values.asJava))
    case Exists(path, exists)          => Filter.operator(naming, path, key, BsonBoolean(exists))
    case Regex(path, pattern, options) =>
      val expression = BsonRegularExpression(pattern, options)
      if path.isEmpty then BsonDocument("$regex", expression) else BsonDocument(path.render(naming), expression)
    case ElemMatch(path, filter)       => Filter.operator(naming, path, key, filter.toBson(naming))
    case All(path, values)             => Filter.operator(naming, path, key, BsonArray(values.asJava))
    case Size(path, size)              => Filter.operator(naming, path, key, BsonInt32(size))
    case Type(path, bsonTypes)         =>
      val names = bsonTypes.map(bsonType => BsonString(bsonType.wireName): BsonValue)
      Filter.operator(naming, path, key, if names.size == 1 then names.head else BsonArray(names.asJava))
    case Mod(path, divisor, remainder) =>
      Filter.operator(naming, path, key, BsonArray(List[BsonValue](BsonInt64(divisor), BsonInt64(remainder)).asJava))
    case Text(search, language)        =>
      val document = BsonDocument("$search", BsonString(search))
      language.foreach(value => document.append("$language", BsonString(value)))
      BsonDocument(key, document)

    case Near(path, geometry, min, max)       => Filter.operator(naming, path, key, Filter.proximity(geometry, min, max))
    case NearSphere(path, geometry, min, max) => Filter.operator(naming, path, key, Filter.proximity(geometry, min, max))

    case GeoWithin(path, shape)        => Filter.operator(naming, path, key, shape.toBson)
    case GeoIntersects(path, geometry) => Filter.operator(naming, path, key, BsonDocument(Geometry.key, geometry.toBson))
    case Expr(expression)              => BsonDocument(key, expression)
    case JsonSchema(schema)            => BsonDocument(key, schema)
    case BitsAllSet(path, mask)        => Filter.operator(naming, path, key, BsonInt64(mask))
    case BitsAnySet(path, mask)        => Filter.operator(naming, path, key, BsonInt64(mask))
    case BitsAllClear(path, mask)      => Filter.operator(naming, path, key, BsonInt64(mask))
    case BitsAnyClear(path, mask)      => Filter.operator(naming, path, key, BsonInt64(mask))
    case And(filters)                  =>
      val rendered = filters.map(_.toBson(naming))
      if filters.forall(Filter.onSelf) && rendered.flatMap(_.keySet.asScala).distinct.size == rendered.map(_.size).sum
      then rendered.foldLeft(BsonDocument())((merged, conjunct) => merged.append(conjunct.getFirstKey, conjunct.get(conjunct.getFirstKey)))
      else BsonDocument(key, BsonArray(rendered.asJava))
    case Or(filters)                   => BsonDocument(key, BsonArray(filters.map(_.toBson(naming)).asJava))
    case Not(filter)                   => BsonDocument(key, BsonArray(List(filter.toBson(naming)).asJava))
    case MatchAll()                    => BsonDocument()
    case MatchNone()                   => BsonDocument(key, BsonArray(List(BsonDocument()).asJava))
    case Raw(document)                 => document

object Filter:

  private val MinDistance = "$minDistance"
  private val MaxDistance = "$maxDistance"

  private def proximity(geometry: Geometry, min: Option[Double], max: Option[Double]): BsonDocument =
    val near = BsonDocument(Geometry.key, geometry.toBson)

    min.foreach(value => near.append(MinDistance, BsonDouble(value)))
    max.foreach(value => near.append(MaxDistance, BsonDouble(value)))

    near
  end proximity

  def all[E]: Filter[E]                                                   = MatchAll()
  def none[E]: Filter[E]                                                  = MatchNone()
  def text[E](search: String, language: Option[String] = None): Filter[E] = Text(search, language)
  def expr[E](expression: BsonDocument): Filter[E]                        = Expr(expression)
  def jsonSchema[E](schema: BsonDocument): Filter[E]                      = JsonSchema(schema)

  def and[E](filters: Filter[E]*): Filter[E] =
    val relevant = filters.toList.flatMap(conjuncts).filterNot(isAll)

    if relevant.exists(isNone)
    then MatchNone()
    else
      relevant match
        case Nil           => MatchAll()
        case single :: Nil => single
        case many          => And(many)
  end and

  def or[E](filters: Filter[E]*): Filter[E] =
    val relevant = filters.toList.flatMap(disjuncts).filterNot(isNone)

    if relevant.exists(isAll)
    then MatchAll()
    else
      relevant match
        case Nil           => MatchNone()
        case single :: Nil => single
        case many          => Or(many)
  end or

  private def conjuncts[E](filter: Filter[E]): List[Filter[E]] =
    filter match
      case And(filters) => filters
      case other        => List(other)

  private def disjuncts[E](filter: Filter[E]): List[Filter[E]] =
    filter match
      case Or(filters) => filters
      case other       => List(other)

  private def isAll[E](filter: Filter[E]): Boolean =
    filter match
      case MatchAll() => true
      case _          => false

  private def isNone[E](filter: Filter[E]): Boolean =
    filter match
      case MatchNone() => true
      case _           => false

  private def operator(
      naming: FieldNaming,
      path: FieldPath,
      name: String,
      value: BsonValue,
  ): BsonDocument =
    if path.isEmpty
    then BsonDocument(name, value)
    else BsonDocument(path.render(naming), BsonDocument(name, value))

  private def onSelf(filter: Filter[?]): Boolean = pathOf(filter).exists(_.isEmpty)

  private def pathOf(filter: Filter[?]): Option[FieldPath] =
    filter match
      case Eq(path, _)        => Some(path)
      case Ne(path, _)        => Some(path)
      case Gt(path, _)        => Some(path)
      case Gte(path, _)       => Some(path)
      case Lt(path, _)        => Some(path)
      case Lte(path, _)       => Some(path)
      case In(path, _)        => Some(path)
      case Nin(path, _)       => Some(path)
      case Exists(path, _)    => Some(path)
      case Regex(path, _, _)  => Some(path)
      case ElemMatch(path, _) => Some(path)
      case All(path, _)       => Some(path)
      case Size(path, _)      => Some(path)
      case Type(path, _)      => Some(path)
      case Mod(path, _, _)    => Some(path)
      case _                  => None
