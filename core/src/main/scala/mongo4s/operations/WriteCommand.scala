package mongo4s.operations

enum WriteCommand[E]:
  case InsertOne[T](document: T)                                            extends WriteCommand[T]
  case ReplaceOne[T](filter: Filter[T], replacement: T, upsert: Boolean)    extends WriteCommand[T]
  case UpdateOne[T](filter: Filter[T], update: Update[T], upsert: Boolean)  extends WriteCommand[T]
  case UpdateMany[T](filter: Filter[T], update: Update[T], upsert: Boolean) extends WriteCommand[T]
  case DeleteOne[T](filter: Filter[T])                                      extends WriteCommand[T]
  case DeleteMany[T](filter: Filter[T])                                     extends WriteCommand[T]

object WriteCommand:
  def replaceOne[E](filter: Filter[E], replacement: E, upsert: Boolean = false): WriteCommand[E] =
    ReplaceOne(filter, replacement, upsert)

  def updateOne[E](filter: Filter[E], update: Update[E], upsert: Boolean = false): WriteCommand[E] =
    UpdateOne(filter, update, upsert)

  def updateMany[E](filter: Filter[E], update: Update[E], upsert: Boolean = false): WriteCommand[E] =
    UpdateMany(filter, update, upsert)
