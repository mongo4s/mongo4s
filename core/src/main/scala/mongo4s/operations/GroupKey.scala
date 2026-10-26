package mongo4s.operations

import org.bson.{BsonDocument, BsonNull, BsonString, BsonValue}

import mongo4s.FieldPath
import mongo4s.bson.FieldNaming

enum GroupKey[E]:
  case All()
  case Path(path: FieldPath)
  case Evaluated(expression: Expr[E, ?])
  case Compound(fields: List[Computed[E]])

  def toBson(naming: FieldNaming): BsonValue =
    this match
      case All()                 => BsonNull.VALUE
      case Path(path)            => BsonString("$" + path.render(naming))
      case Evaluated(expression) => expression.toBson(naming)
      case Compound(fields)      => fields.foldLeft(BsonDocument())((key, field) => key.append(field.path.render(naming), field.expression.toBson(naming)))
