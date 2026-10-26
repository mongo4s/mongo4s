package mongo4s.operations

import java.time.Instant

import org.bson.{BsonArray, BsonBoolean, BsonDocument, BsonInt32, BsonString, BsonTimestamp, BsonValue}

import mongo4s.bson.{BsonEncoder, FieldNaming}
import mongo4s.{BitwiseOf, ComparableOf, ElementOf, Field, FieldPath, NumericOf}

enum Update[E](val key: String):
  case Set[T](path: FieldPath, value: BsonValue)                   extends Update[T]("$set")
  case SetOnInsert[T](path: FieldPath, value: BsonValue)           extends Update[T]("$setOnInsert")
  case Unset[T](path: FieldPath)                                   extends Update[T]("$unset")
  case Rename[T](path: FieldPath, to: FieldPath)                   extends Update[T]("$rename")
  case Inc[T](path: FieldPath, amount: BsonValue)                  extends Update[T]("$inc")
  case Mul[T](path: FieldPath, factor: BsonValue)                  extends Update[T]("$mul")
  case Min[T](path: FieldPath, value: BsonValue)                   extends Update[T]("$min")
  case Max[T](path: FieldPath, value: BsonValue)                   extends Update[T]("$max")
  case CurrentDate[T](path: FieldPath, asTimestamp: Boolean)       extends Update[T]("$currentDate")
  case Push[T](path: FieldPath, value: BsonValue)                  extends Update[T]("$push")
  case Pull[T](path: FieldPath, value: BsonValue)                  extends Update[T]("$pull")
  case PullAll[T](path: FieldPath, values: BsonArray)              extends Update[T]("$pullAll")
  case PullWhere[T, U](path: FieldPath, condition: Filter[U])      extends Update[T]("$pull")
  case Pop[T](path: FieldPath, first: Boolean)                     extends Update[T]("$pop")
  case AddToSet[T](path: FieldPath, value: BsonValue)              extends Update[T]("$addToSet")
  case AddToSetEach[T](path: FieldPath, values: BsonArray)         extends Update[T]("$addToSet")
  case Combine[T](updates: List[Update[T]])                        extends Update[T]("")
  case Raw[T](document: BsonDocument)                              extends Update[T]("")
  case Pipeline[T](stages: List[Stage[T]])                         extends Update[T]("")
  case Bit[T](path: FieldPath, operation: String, mask: BsonValue) extends Update[T]("$bit")

  case PushEach[T](
      path: FieldPath,
      values: BsonArray,
      options: PushOptions[?],
  ) extends Update[T]("$push")

  def and(other: Update[E]): Update[E] = (this, other) match
    case (Pipeline(left), Pipeline(right)) => Pipeline(left ++ right)
    case (Combine(left), Combine(right))   => Combine(left ++ right)
    case (Combine(left), single)           => Combine(left :+ single)
    case (single, Combine(right))          => Combine(single :: right)
    case (left, right)                     => Combine(List(left, right))

  def toPipeline(naming: FieldNaming): Option[List[BsonDocument]] =
    this match
      case Pipeline(stages) => Some(stages.map(_.toBson(naming)))
      case _                => None

  def toBson(naming: FieldNaming): BsonDocument =
    val document = BsonDocument()

    Update.write(this, naming, document)

    if document.isEmpty
    then throw IllegalArgumentException("Update is empty — MongoDB rejects an update document with no operators")

    Update.requireDisjoint(document)

    document
  end toBson

object Update:

  def set[E, A](field: Field[E, A], value: A)(using
      encoder: BsonEncoder[A],
  ): Update[E] =
    Set(field.path, encoder.encode(value))

  def setOnInsert[E, A](field: Field[E, A], value: A)(using
      encoder: BsonEncoder[A],
  ): Update[E] =
    SetOnInsert(field.path, encoder.encode(value))

  def unset[E, A](field: Field[E, A]): Update[E] = Unset(field.path)

  def rename[E, A](field: Field[E, A], to: Field[E, A]): Update[E] = Rename(field.path, to.path)

  def inc[E, C, A](field: Field[E, C], amount: A)(using
      numeric: NumericOf[C, A],
  ): Update[E] =
    Inc(field.path, numeric.encode(amount))

  def mul[E, C, A](field: Field[E, C], factor: A)(using
      numeric: NumericOf[C, A],
  ): Update[E] =
    Mul(field.path, numeric.encode(factor))

  def min[E, C, A](field: Field[E, C], value: A)(using
      comparable: ComparableOf[C, A],
  ): Update[E] =
    Min(field.path, comparable.encode(value))

  def max[E, C, A](field: Field[E, C], value: A)(using
      comparable: ComparableOf[C, A],
  ): Update[E] =
    Max(field.path, comparable.encode(value))

  def currentDate[E](field: Field[E, Instant]): Update[E]            = CurrentDate(field.path, asTimestamp = false)
  def currentTimestamp[E](field: Field[E, BsonTimestamp]): Update[E] = CurrentDate(field.path, asTimestamp = true)

  def push[E, C, A](field: Field[E, C], value: A)(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    Push(field.path, summon[BsonEncoder[A]].encode(value))

  def pushAll[E, C, A](
      field: Field[E, C],
      values: Seq[A],
      options: PushOptions[A] = PushOptions.default[A],
  )(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    PushEach(field.path, bsonArray(values), options)

  def pull[E, C, A](field: Field[E, C], value: A)(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    Pull(field.path, summon[BsonEncoder[A]].encode(value))

  def pullAll[E, C, A](field: Field[E, C], values: Seq[A])(using ElementOf[C, A], BsonEncoder[A]): Update[E] =
    PullAll(field.path, bsonArray(values))

  def pullWhere[E, C, A](field: Field[E, C], condition: Filter[A])(using ElementOf[C, A]): Update[E] =
    PullWhere(field.path, condition)

  def addToSet[E, C, A](field: Field[E, C], value: A)(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    AddToSet(field.path, summon[BsonEncoder[A]].encode(value))

  def addAllToSet[E, C, A](field: Field[E, C], values: Seq[A])(using
      ElementOf[C, A],
      BsonEncoder[A],
  ): Update[E] =
    AddToSetEach(field.path, bsonArray(values))

  def popFirst[E, C, A](field: Field[E, C])(using ElementOf[C, A]): Update[E] = Pop(field.path, first = true)
  def popLast[E, C, A](field: Field[E, C])(using ElementOf[C, A]): Update[E]  = Pop(field.path, first = false)

  def bitAnd[E, C, A](field: Field[E, C], mask: A)(using bits: BitwiseOf[C, A]): Update[E] = Bit(field.path, "and", bits.encode(mask))
  def bitOr[E, C, A](field: Field[E, C], mask: A)(using bits: BitwiseOf[C, A]): Update[E]  = Bit(field.path, "or", bits.encode(mask))
  def bitXor[E, C, A](field: Field[E, C], mask: A)(using bits: BitwiseOf[C, A]): Update[E] = Bit(field.path, "xor", bits.encode(mask))

  def combine[E](updates: Update[E]*): Update[E] = Combine(updates.toList)

  def pipeline[E](stages: Stage[E]*): Update[E] =
    require(stages.nonEmpty, "A pipeline update needs at least one stage")

    stages.foreach {
      case _: (Stage.AddFields[E] | Stage.ProjectStage[E] | Stage.Unset[E] | Stage.ReplaceRoot[E] | Stage.Raw[E]) => ()
      case other                                                                                                  =>
        throw IllegalArgumentException(s"A pipeline update takes $$addFields, $$project, $$unset and $$replaceRoot stages, not ${other.key}")
    }

    Pipeline(stages.toList)
  end pipeline

  private def bsonArray[A](values: Seq[A])(using encoder: BsonEncoder[A]): BsonArray =
    val array = BsonArray()

    values.foreach(value => array.add(encoder.encode(value)))

    array
  end bsonArray

  private def write[E](update: Update[E], naming: FieldNaming, target: BsonDocument): Unit = update match
    case Set(path, value)                => operator(target, update.key, path.render(naming), value)
    case SetOnInsert(path, value)        => operator(target, update.key, path.render(naming), value)
    case Unset(path)                     => operator(target, update.key, path.render(naming), BsonString(""))
    case Rename(path, to)                => operator(target, update.key, path.render(naming), BsonString(to.render(naming)))
    case Inc(path, amount)               => operator(target, update.key, path.render(naming), amount)
    case Mul(path, factor)               => operator(target, update.key, path.render(naming), factor)
    case Min(path, value)                => operator(target, update.key, path.render(naming), value)
    case Max(path, value)                => operator(target, update.key, path.render(naming), value)
    case CurrentDate(path, asTimestamp)  =>
      val kind = if asTimestamp then BsonDocument("$type", BsonString("timestamp")) else BsonBoolean(true)
      operator(target, update.key, path.render(naming), kind)
    case Push(path, value)               => operator(target, update.key, path.render(naming), value)
    case PushEach(path, values, options) => operator(target, update.key, path.render(naming), each(values, options, naming))
    case Pull(path, value)               => operator(target, update.key, path.render(naming), value)
    case PullAll(path, values)           => operator(target, update.key, path.render(naming), values)
    case PullWhere(path, condition)      => operator(target, update.key, path.render(naming), condition.toBson(naming))
    case Pop(path, first)                => operator(target, update.key, path.render(naming), BsonInt32(if first then -1 else 1))
    case Bit(path, operation, mask)      => operator(target, update.key, path.render(naming), BsonDocument(operation, mask))
    case AddToSet(path, value)           => operator(target, update.key, path.render(naming), value)
    case AddToSetEach(path, vs)          => operator(target, update.key, path.render(naming), each(vs))
    case Combine(updates)                => updates.foreach(write(_, naming, target))
    case Raw(document)                   => document.forEach((name, value) => mergeOperator(target, name, value))
    case Pipeline(_)                     =>
      throw IllegalArgumentException("A pipeline update goes to the server as stages, not operators — it cannot be rendered as, or combined with, an operator update")

  private val Each      = "$each"
  private val Position  = "$position"
  private val PushSlice = "$slice"
  private val PushSort  = "$sort"

  private def each(values: BsonArray): BsonDocument = BsonDocument(Each, values)

  private def each(values: BsonArray, options: PushOptions[?], naming: FieldNaming): BsonDocument =
    val document = each(values)

    options.position.foreach(value => document.append(Position, BsonInt32(value)): Unit)
    options.slice.foreach(value => document.append(PushSlice, BsonInt32(value)): Unit)
    options.sort.foreach(value => document.append(PushSort, value.toBson(naming)): Unit)
    options.sortScalars.foreach(ascending => document.append(PushSort, BsonInt32(if ascending then 1 else -1)): Unit)

    document
  end each

  private def operator(target: BsonDocument, name: String, path: String, value: BsonValue): Unit =
    existingOperator(target, name) match
      case Some(existing) => put(existing, name, path, value)
      case None           => target.put(name, BsonDocument(path, value)): Unit

  private def put(changes: BsonDocument, name: String, path: String, value: BsonValue): Unit =
    if changes.containsKey(path)
    then throw IllegalArgumentException(s"Update changes '$path' twice under $name — MongoDB keeps one change per path, so the first would be lost")

    changes.put(path, value): Unit
  end put

  private def requireDisjoint(document: BsonDocument): Unit =
    val touched  = scala.collection.mutable.Set.empty[String]
    val enclosed = scala.collection.mutable.Map.empty[String, String]

    def overlap(outer: String, inner: String): Nothing =
      throw IllegalArgumentException(s"Update changes both '$outer' and '$inner' — MongoDB refuses overlapping paths as a conflict")

    def touch(path: String): Unit =
      val prefixes = path.split('.').inits.drop(1).filter(_.nonEmpty).map(_.mkString(".")).toList

      prefixes.find(touched.contains).foreach(overlap(_, path))
      enclosed.get(path).foreach(overlap(path, _))

      if !touched.add(path)
      then throw IllegalArgumentException(s"Update changes '$path' under two operators — MongoDB refuses that as a conflict")

      prefixes.foreach(prefix => enclosed.getOrElseUpdate(prefix, path): Unit)
    end touch

    document.forEach { (name, changes) =>
      changes match
        case paths: BsonDocument =>
          paths.forEach { (path, value) =>
            touch(path)
            if name == RenameKey && value.isString then touch(value.asString.getValue)
          }
        case _                   => ()
    }
  end requireDisjoint

  private val RenameKey = "$rename"

  private def mergeOperator(target: BsonDocument, name: String, value: BsonValue): Unit =
    (existingOperator(target, name), value) match
      case (None, incoming: BsonDocument)           => target.put(name, incoming.clone()): Unit
      case (None, other)                            => target.put(name, other): Unit
      case (Some(existing), incoming: BsonDocument) => incoming.forEach((path, v) => put(existing, name, path, v))
      case (Some(_), other)                         =>
        throw IllegalArgumentException(
          s"Cannot merge operator '$name': already present as a document, but the raw update supplies $other"
        )

  private def existingOperator(target: BsonDocument, name: String): Option[BsonDocument] =
    Option(target.get(name)).collect { case document: BsonDocument => document }
