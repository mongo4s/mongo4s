package mongo4s.operations

import java.time.Instant

import org.bson.{BsonArray, BsonBoolean, BsonDocument, BsonInt32, BsonString, BsonValue}

import mongo4s.bson.{BsonEncoder, FieldNaming}
import mongo4s.{ElementOf, Field, FieldPath, NumericOf}

enum Update[E]:
  case Set[T](path: FieldPath, value: BsonValue)                                extends Update[T]
  case SetOnInsert[T](path: FieldPath, value: BsonValue)                        extends Update[T]
  case Unset[T](path: FieldPath)                                                extends Update[T]
  case Rename[T](path: FieldPath, to: FieldPath)                                extends Update[T]
  case Inc[T](path: FieldPath, amount: BsonValue)                               extends Update[T]
  case Mul[T](path: FieldPath, factor: BsonValue)                               extends Update[T]
  case Min[T](path: FieldPath, value: BsonValue)                                extends Update[T]
  case Max[T](path: FieldPath, value: BsonValue)                                extends Update[T]
  case CurrentDate[T](path: FieldPath)                                          extends Update[T]
  case Push[T](path: FieldPath, value: BsonValue)                               extends Update[T]
  case PushEach[T](path: FieldPath, values: BsonArray, options: PushOptions[?]) extends Update[T]
  case Pull[T](path: FieldPath, value: BsonValue)                               extends Update[T]
  case PullAll[T](path: FieldPath, values: BsonArray)                           extends Update[T]
  case Pop[T](path: FieldPath, first: Boolean)                                  extends Update[T]
  case AddToSet[T](path: FieldPath, value: BsonValue)                           extends Update[T]
  case AddToSetEach[T](path: FieldPath, values: BsonArray)                      extends Update[T]
  case Combine[T](updates: List[Update[T]])                                     extends Update[T]
  case Raw[T](document: BsonDocument)                                           extends Update[T]

  def and(other: Update[E]): Update[E] = (this, other) match
    case (Combine(left), Combine(right)) => Combine(left ++ right)
    case (Combine(left), single)         => Combine(left :+ single)
    case (single, Combine(right))        => Combine(single :: right)
    case (left, right)                   => Combine(List(left, right))

  def toBson(naming: FieldNaming): BsonDocument =
    val document = BsonDocument()

    Update.write(this, naming, document)

    if document.isEmpty
    then throw IllegalArgumentException("Update is empty — MongoDB rejects an update document with no operators")

    document
  end toBson

object Update:

  def set[E, A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): Update[E] =
    Set(field.path, encoder.encode(value))

  def setOnInsert[E, A](field: Field[E, A], value: A)(using encoder: BsonEncoder[A]): Update[E] =
    SetOnInsert(field.path, encoder.encode(value))

  def unset[E, A](field: Field[E, A]): Update[E] = Unset(field.path)

  def rename[E, A](field: Field[E, A], to: Field[E, A]): Update[E] = Rename(field.path, to.path)

  def inc[E, C, A](field: Field[E, C], amount: A)(using numeric: NumericOf[C, A]): Update[E] =
    Inc(field.path, numeric.encode(amount))

  def mul[E, C, A](field: Field[E, C], factor: A)(using numeric: NumericOf[C, A]): Update[E] =
    Mul(field.path, numeric.encode(factor))

  def min[E, C, A](field: Field[E, C], value: A)(using numeric: NumericOf[C, A]): Update[E] =
    Min(field.path, numeric.encode(value))

  def max[E, C, A](field: Field[E, C], value: A)(using numeric: NumericOf[C, A]): Update[E] =
    Max(field.path, numeric.encode(value))

  def currentDate[E](field: Field[E, Instant]): Update[E] = CurrentDate(field.path)

  def push[E, C, A](field: Field[E, C], value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    Push(field.path, summon[BsonEncoder[A]].encode(value))

  def pushAll[E, C, A](field: Field[E, C], values: Seq[A], options: PushOptions[A] = PushOptions.default[A])(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    PushEach(field.path, bsonArray(values), options)

  def pull[E, C, A](field: Field[E, C], value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    Pull(field.path, summon[BsonEncoder[A]].encode(value))

  def pullAll[E, C, A](field: Field[E, C], values: Seq[A])(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    PullAll(field.path, bsonArray(values))

  def addToSet[E, C, A](field: Field[E, C], value: A)(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    AddToSet(field.path, summon[BsonEncoder[A]].encode(value))

  def addAllToSet[E, C, A](field: Field[E, C], values: Seq[A])(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    AddToSetEach(field.path, bsonArray(values))

  def popFirst[E, C](field: Field[E, C]): Update[E] = Pop(field.path, first = true)
  def popLast[E, C](field: Field[E, C]): Update[E]  = Pop(field.path, first = false)

  def combine[E](updates: Update[E]*): Update[E] = Combine(updates.toList)

  private def bsonArray[A](values: Seq[A])(using encoder: BsonEncoder[A]): BsonArray =
    val array = BsonArray()

    values.foreach(value => array.add(encoder.encode(value)))

    array
  end bsonArray

  private def write[E](update: Update[E], naming: FieldNaming, target: BsonDocument): Unit = update match
    case Set(path, value)                => operator(target, "$set", path.render(naming), value)
    case SetOnInsert(path, value)        => operator(target, "$setOnInsert", path.render(naming), value)
    case Unset(path)                     => operator(target, "$unset", path.render(naming), BsonString(""))
    case Rename(path, to)                => operator(target, "$rename", path.render(naming), BsonString(to.render(naming)))
    case Inc(path, amount)               => operator(target, "$inc", path.render(naming), amount)
    case Mul(path, factor)               => operator(target, "$mul", path.render(naming), factor)
    case Min(path, value)                => operator(target, "$min", path.render(naming), value)
    case Max(path, value)                => operator(target, "$max", path.render(naming), value)
    case CurrentDate(path)               => operator(target, "$currentDate", path.render(naming), BsonBoolean(true))
    case Push(path, value)               => operator(target, "$push", path.render(naming), value)
    case PushEach(path, values, options) => operator(target, "$push", path.render(naming), each(values, options, naming))
    case Pull(path, value)               => operator(target, "$pull", path.render(naming), value)
    case PullAll(path, values)           => operator(target, "$pullAll", path.render(naming), values)
    case Pop(path, first)                => operator(target, "$pop", path.render(naming), BsonInt32(if first then -1 else 1))
    case AddToSet(path, value)           => operator(target, "$addToSet", path.render(naming), value)
    case AddToSetEach(path, vs)          => operator(target, "$addToSet", path.render(naming), each(vs))
    case Combine(updates)                => updates.foreach(write(_, naming, target))
    case Raw(document)                   => document.forEach((name, value) => mergeOperator(target, name, value))

  private def each(values: BsonArray): BsonDocument = BsonDocument("$each", values)

  private def each(values: BsonArray, options: PushOptions[?], naming: FieldNaming): BsonDocument =
    val document = BsonDocument("$each", values)

    options.position.foreach(value => document.append("$position", BsonInt32(value)): Unit)
    options.slice.foreach(value => document.append("$slice", BsonInt32(value)): Unit)
    options.sort.foreach(value => document.append("$sort", value.toBson(naming)): Unit)
    options.sortScalars.foreach(ascending => document.append("$sort", BsonInt32(if ascending then 1 else -1)): Unit)

    document
  end each

  private def operator(target: BsonDocument, name: String, path: String, value: BsonValue): Unit =
    existingOperator(target, name) match
      case Some(existing) => existing.put(path, value): Unit
      case None           => target.put(name, BsonDocument(path, value)): Unit

  private def mergeOperator(target: BsonDocument, name: String, value: BsonValue): Unit =
    (existingOperator(target, name), value) match
      case (None, incoming: BsonDocument)           => target.put(name, incoming.clone()): Unit
      case (None, other)                            => target.put(name, other): Unit
      case (Some(existing), incoming: BsonDocument) => incoming.forEach((path, v) => existing.put(path, v): Unit)
      case (Some(_), other)                         =>
        throw IllegalArgumentException(
          s"Cannot merge operator '$name': already present as a document, but the raw update supplies $other"
        )

  private def existingOperator(target: BsonDocument, name: String): Option[BsonDocument] =
    Option(target.get(name)).collect { case document: BsonDocument => document }
