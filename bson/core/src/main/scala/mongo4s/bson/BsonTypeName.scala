package mongo4s.bson

import org.bson.{BsonType, BsonValue}

enum BsonTypeName(val wireName: String):
  case EndOfDocument       extends BsonTypeName("endOfDocument")
  case Double              extends BsonTypeName("double")
  case String              extends BsonTypeName("string")
  case Object              extends BsonTypeName("object")
  case Array               extends BsonTypeName("array")
  case BinData             extends BsonTypeName("binData")
  case Undefined           extends BsonTypeName("undefined")
  case ObjectId            extends BsonTypeName("objectId")
  case Bool                extends BsonTypeName("bool")
  case Date                extends BsonTypeName("date")
  case Null                extends BsonTypeName("null")
  case Regex               extends BsonTypeName("regex")
  case DbPointer           extends BsonTypeName("dbPointer")
  case Javascript          extends BsonTypeName("javascript")
  case Symbol              extends BsonTypeName("symbol")
  case JavascriptWithScope extends BsonTypeName("javascriptWithScope")
  case Int                 extends BsonTypeName("int")
  case Timestamp           extends BsonTypeName("timestamp")
  case Long                extends BsonTypeName("long")
  case Decimal             extends BsonTypeName("decimal")
  case MinKey              extends BsonTypeName("minKey")
  case MaxKey              extends BsonTypeName("maxKey")

object BsonTypeName:

  def of(value: BsonValue): BsonTypeName = of(value.getBsonType)

  def of(tpe: BsonType): BsonTypeName = tpe match
    case BsonType.END_OF_DOCUMENT       => EndOfDocument
    case BsonType.DOUBLE                => Double
    case BsonType.STRING                => String
    case BsonType.DOCUMENT              => Object
    case BsonType.ARRAY                 => Array
    case BsonType.BINARY                => BinData
    case BsonType.UNDEFINED             => Undefined
    case BsonType.OBJECT_ID             => ObjectId
    case BsonType.BOOLEAN               => Bool
    case BsonType.DATE_TIME             => Date
    case BsonType.NULL                  => Null
    case BsonType.REGULAR_EXPRESSION    => Regex
    case BsonType.DB_POINTER            => DbPointer
    case BsonType.JAVASCRIPT            => Javascript
    case BsonType.SYMBOL                => Symbol
    case BsonType.JAVASCRIPT_WITH_SCOPE => JavascriptWithScope
    case BsonType.INT32                 => Int
    case BsonType.TIMESTAMP             => Timestamp
    case BsonType.INT64                 => Long
    case BsonType.DECIMAL128            => Decimal
    case BsonType.MIN_KEY               => MinKey
    case BsonType.MAX_KEY               => MaxKey
