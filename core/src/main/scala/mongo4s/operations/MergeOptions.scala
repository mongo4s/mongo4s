package mongo4s.operations

import org.bson.BsonDocument

final class MergeOptions private (
    val database: Option[String],
    val on: List[String],
    val whenMatched: Option[MergeOptions.WhenMatched],
    val whenNotMatched: Option[MergeOptions.WhenNotMatched],
    val let: Option[BsonDocument],
):
  def inDatabase(value: String): MergeOptions = copy(database = Some(value))

  def onFields(fields: List[String]): MergeOptions = copy(on = fields)

  def whenMatched(value: MergeOptions.WhenMatched): MergeOptions = copy(whenMatched = Some(value))

  def whenNotMatched(value: MergeOptions.WhenNotMatched): MergeOptions = copy(whenNotMatched = Some(value))

  def withLet(value: BsonDocument): MergeOptions = copy(let = Some(value))

  def isEmpty: Boolean =
    database.isEmpty && on.isEmpty && whenMatched.isEmpty && whenNotMatched.isEmpty && let.isEmpty

  private def copy(
      database: Option[String] = database,
      on: List[String] = on,
      whenMatched: Option[MergeOptions.WhenMatched] = whenMatched,
      whenNotMatched: Option[MergeOptions.WhenNotMatched] = whenNotMatched,
      let: Option[BsonDocument] = let,
  ): MergeOptions =
    new MergeOptions(
      database = database,
      on = on,
      whenMatched = whenMatched,
      whenNotMatched = whenNotMatched,
      let = let,
    )

object MergeOptions:

  enum WhenMatched(val wireName: String):
    case Replace      extends WhenMatched("replace")
    case KeepExisting extends WhenMatched("keepExisting")
    case Merge        extends WhenMatched("merge")
    case Fail         extends WhenMatched("fail")

  enum WhenNotMatched(val wireName: String):
    case Insert  extends WhenNotMatched("insert")
    case Discard extends WhenNotMatched("discard")
    case Fail    extends WhenNotMatched("fail")

  val default: MergeOptions =
    new MergeOptions(
      database = None,
      on = Nil,
      whenMatched = None,
      whenNotMatched = None,
      let = None,
    )
