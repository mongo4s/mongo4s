package mongo4s.operations

enum WriteCommand[E]:
  case InsertOne[T](document: T)                                                   extends WriteCommand[T]
  case ReplaceOne[T](filter: Filter[T], replacement: T, options: ReplaceOptions)   extends WriteCommand[T]
  case UpdateOne[T](filter: Filter[T], update: Update[T], options: UpdateOptions)  extends WriteCommand[T]
  case UpdateMany[T](filter: Filter[T], update: Update[T], options: UpdateOptions) extends WriteCommand[T]
  case DeleteOne[T](filter: Filter[T])                                             extends WriteCommand[T]
  case DeleteMany[T](filter: Filter[T])                                            extends WriteCommand[T]

object WriteCommand:
  def replaceOne[E](filter: Filter[E], replacement: E, options: ReplaceOptions = ReplaceOptions.default): WriteCommand[E] =
    ReplaceOne(filter, replacement, options)

  def updateOne[E](filter: Filter[E], update: Update[E], options: UpdateOptions = UpdateOptions.default): WriteCommand[E] =
    UpdateOne(filter, update, options)

  def updateMany[E](filter: Filter[E], update: Update[E], options: UpdateOptions = UpdateOptions.default): WriteCommand[E] =
    UpdateMany(filter, update, options)
