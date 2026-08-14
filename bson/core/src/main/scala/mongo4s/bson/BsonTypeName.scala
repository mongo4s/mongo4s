package mongo4s.bson

import org.bson.{BsonType, BsonValue}

private[mongo4s] object BsonTypeName:

  val Double: String    = "double"
  val String: String    = "string"
  val Object: String    = "object"
  val Array: String     = "array"
  val BinData: String   = "binData"
  val ObjectId: String  = "objectId"
  val Bool: String      = "bool"
  val Date: String      = "date"
  val Null: String      = "null"
  val Regex: String     = "regex"
  val Int: String       = "int"
  val Timestamp: String = "timestamp"
  val Long: String      = "long"
  val Decimal: String   = "decimal"
  val MinKey: String    = "minKey"
  val MaxKey: String    = "maxKey"

  def of(value: BsonValue): String = of(value.getBsonType)

  def of(tpe: BsonType): String = tpe match
    case BsonType.END_OF_DOCUMENT       => "endOfDocument"
    case BsonType.DOUBLE                => Double
    case BsonType.STRING                => String
    case BsonType.DOCUMENT              => Object
    case BsonType.ARRAY                 => Array
    case BsonType.BINARY                => BinData
    case BsonType.UNDEFINED             => "undefined"
    case BsonType.OBJECT_ID             => ObjectId
    case BsonType.BOOLEAN               => Bool
    case BsonType.DATE_TIME             => Date
    case BsonType.NULL                  => Null
    case BsonType.REGULAR_EXPRESSION    => Regex
    case BsonType.DB_POINTER            => "dbPointer"
    case BsonType.JAVASCRIPT            => "javascript"
    case BsonType.SYMBOL                => "symbol"
    case BsonType.JAVASCRIPT_WITH_SCOPE => "javascriptWithScope"
    case BsonType.INT32                 => Int
    case BsonType.TIMESTAMP             => Timestamp
    case BsonType.INT64                 => Long
    case BsonType.DECIMAL128            => Decimal
    case BsonType.MIN_KEY               => MinKey
    case BsonType.MAX_KEY               => MaxKey
