package mongo4s.results

import org.bson.{BsonDocument, BsonValue}

import scala.jdk.CollectionConverters.given

final case class ExecutionSummary(
    returned: Long,
    keysExamined: Long,
    docsExamined: Long,
    durationMillis: Long,
)

final case class ExplainSummary(
    indexes: List[String],
    stages: List[String],
    execution: Option[ExecutionSummary],
):
  def usedIndex: Boolean         = indexes.nonEmpty
  def scannedCollection: Boolean = stages.contains(ExplainSummary.CollectionScan)
  def sortedInMemory: Boolean    = stages.contains(ExplainSummary.BlockingSort)

object ExplainSummary:
  val CollectionScan: String = "COLLSCAN"
  val BlockingSort: String   = "SORT"

  private val StageField     = "stage"
  private val IndexField     = "indexName"
  private val ExecutionField = "executionStats"

  def of(explain: BsonDocument): ExplainSummary =
    val stages  = List.newBuilder[String]
    val indexes = List.newBuilder[String]

    var execution: Option[ExecutionSummary] = None

    def string(document: BsonDocument, name: String): Option[String] =
      Option(document.get(name)).collect { case value if value.isString => value.asString.getValue }

    def number(document: BsonDocument, name: String): Long =
      Option(document.get(name)).collect { case value if value.isNumber => value.asNumber.longValue }.getOrElse(0L)

    def visit(value: BsonValue): Unit = value match
      case document: BsonDocument =>
        string(document, StageField).foreach(stages += _)
        string(document, IndexField).foreach(indexes += _)

        if execution.isEmpty
        then
          Option(document.get(ExecutionField)).collect { case stats if stats.isDocument => stats.asDocument }.foreach { stats =>
            execution = Some(
              ExecutionSummary(
                returned = number(stats, "nReturned"),
                keysExamined = number(stats, "totalKeysExamined"),
                docsExamined = number(stats, "totalDocsExamined"),
                durationMillis = number(stats, "executionTimeMillis"),
              )
            )
          }

        document.values.asScala.foreach(visit)

      case array if array.isArray => array.asArray.getValues.asScala.foreach(visit)
      case _                      => ()

    visit(explain)

    ExplainSummary(
      indexes = indexes.result().distinct,
      stages = stages.result().distinct,
      execution = execution,
    )
  end of
