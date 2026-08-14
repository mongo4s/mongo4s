package mongo4s.operations

import org.bson.{BsonDocument, BsonInt32, BsonValue}

import mongo4s.{Field, FieldPath}
import mongo4s.bson.{BsonEncoder, FieldNaming}

enum Update[E]:
  case Set[T](path: FieldPath, value: BsonValue)      extends Update[T]
  case Unset[T](path: FieldPath)                      extends Update[T]
  case Inc[T](path: FieldPath, amount: BsonValue)     extends Update[T]
  case Push[T](path: FieldPath, value: BsonValue)     extends Update[T]
  case Pull[T](path: FieldPath, value: BsonValue)     extends Update[T]
  case AddToSet[T](path: FieldPath, value: BsonValue) extends Update[T]
  case Combine[T](updates: List[Update[T]])           extends Update[T]
  case Raw[T](document: BsonDocument)                 extends Update[T]

  def and(other: Update[E]): Update[E] = Combine(List(this, other))

  def toBson(naming: FieldNaming): BsonDocument =
    val document = BsonDocument()
    Update.write(this, naming, document)
    document

object Update:
  def set[E, A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): Update[E] =
    Set(field.path, encoder.encode(value))

  def unset[E, A](field: Field[E, A]): Update[E] = Unset(field.path)

  def inc[E, A](field: Field[E, A], amount: Int): Update[E] = Inc(field.path, BsonInt32(amount))

  def push[E, A](field: Field[E, Seq[A]], value: A)(using encoder: BsonEncoder[A]): Update[E] =
    Push(field.path, encoder.encode(value))

  def pull[E, A](field: Field[E, Seq[A]], value: A)(using encoder: BsonEncoder[A]): Update[E] =
    Pull(field.path, encoder.encode(value))

  def combine[E](updates: Update[E]*): Update[E] = Combine(updates.toList)

  private def write[E](update: Update[E], naming: FieldNaming, target: BsonDocument): Unit = update match
    case Set(path, value)      => operator(target, "$set", path.render(naming), value)
    case Unset(path)           => operator(target, "$unset", path.render(naming), BsonInt32(1))
    case Inc(path, amount)     => operator(target, "$inc", path.render(naming), amount)
    case Push(path, value)     => operator(target, "$push", path.render(naming), value)
    case Pull(path, value)     => operator(target, "$pull", path.render(naming), value)
    case AddToSet(path, value) => operator(target, "$addToSet", path.render(naming), value)
    case Combine(updates)      => updates.foreach(write(_, naming, target))
    case Raw(document)         => document.forEach((key, value) => target.append(key, value))

  private def operator(target: BsonDocument, name: String, path: String, value: BsonValue): Unit =
    Option(target.getDocument(name, null)) match
      case Some(existing) => existing.append(path, value)
      case None           => target.append(name, BsonDocument(path, value))
