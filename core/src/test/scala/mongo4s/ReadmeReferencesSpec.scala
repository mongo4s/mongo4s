package mongo4s

import java.nio.file.{Files, Path}

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

object ReadmeReferencesSpec:

  /** The library objects whose members the README names, and the source that defines them. */
  val known: Map[String, String] = Map(
    "Accumulator"              -> "core/src/main/scala/mongo4s/operations/Accumulator.scala",
    "BulkWriteOptions"         -> "core/src/main/scala/mongo4s/operations/BulkWriteOptions.scala",
    "BucketGranularity"        -> "core/src/main/scala/mongo4s/operations/BucketGranularity.scala",
    "CountOptions"             -> "core/src/main/scala/mongo4s/operations/CountOptions.scala",
    "CreateCollectionOptions"  -> "core/src/main/scala/mongo4s/operations/CreateCollectionOptions.scala",
    "DeleteOptions"            -> "core/src/main/scala/mongo4s/operations/DeleteOptions.scala",
    "DensifyBounds"            -> "core/src/main/scala/mongo4s/operations/DensifyRange.scala",
    "DensifyRange"             -> "core/src/main/scala/mongo4s/operations/DensifyRange.scala",
    "Effect"                   -> "core/src/main/scala/mongo4s/Effect.scala",
    "EstimatedCountOptions"    -> "core/src/main/scala/mongo4s/operations/EstimatedCountOptions.scala",
    "ExplainSummary"           -> "core/src/main/scala/mongo4s/results/ExplainSummary.scala",
    "Field"                    -> "core/src/main/scala/mongo4s/Field.scala",
    "Expr"                     -> "core/src/main/scala/mongo4s/operations/Expr.scala",
    "FieldNaming"              -> "bson/core/src/main/scala/mongo4s/bson/FieldNaming.scala",
    "Filter"                   -> "core/src/main/scala/mongo4s/operations/Filter.scala",
    "FindOneAndDeleteOptions"  -> "core/src/main/scala/mongo4s/operations/FindOneAndDeleteOptions.scala",
    "FindOneAndReplaceOptions" -> "core/src/main/scala/mongo4s/operations/FindOneAndReplaceOptions.scala",
    "FillOutput"               -> "core/src/main/scala/mongo4s/operations/FillOutput.scala",
    "FindOneAndUpdateOptions"  -> "core/src/main/scala/mongo4s/operations/FindOneAndUpdateOptions.scala",
    "GeoNearOptions"           -> "core/src/main/scala/mongo4s/operations/GeoNearOptions.scala",
    "Index"                    -> "core/src/main/scala/mongo4s/operations/Index.scala",
    "InsertManyOptions"        -> "core/src/main/scala/mongo4s/operations/InsertManyOptions.scala",
    "InsertOneOptions"         -> "core/src/main/scala/mongo4s/operations/InsertOneOptions.scala",
    "MergeOptions"             -> "core/src/main/scala/mongo4s/operations/MergeOptions.scala",
    "MongoClient"              -> "core/src/main/scala/mongo4s/MongoClient.scala",
    "OutOptions"               -> "core/src/main/scala/mongo4s/operations/OutOptions.scala",
    "PrimaryKey"               -> "core/src/main/scala/mongo4s/PrimaryKey.scala",
    "Projection"               -> "core/src/main/scala/mongo4s/operations/Projection.scala",
    "PushOptions"              -> "core/src/main/scala/mongo4s/operations/PushOptions.scala",
    "ReplaceOptions"           -> "core/src/main/scala/mongo4s/operations/ReplaceOptions.scala",
    "RsBridgeConfig"           -> "core/src/main/scala/mongo4s/RsBridgeConfig.scala",
    "SearchCount"              -> "core/src/main/scala/mongo4s/operations/SearchOptions.scala",
    "SearchIndex"              -> "core/src/main/scala/mongo4s/operations/SearchIndex.scala",
    "SearchOperator"           -> "core/src/main/scala/mongo4s/operations/SearchOperator.scala",
    "SearchOptions"            -> "core/src/main/scala/mongo4s/operations/SearchOptions.scala",
    "Sort"                     -> "core/src/main/scala/mongo4s/operations/Sort.scala",
    "Stage"                    -> "core/src/main/scala/mongo4s/operations/Stage.scala",
    "TransactionOptions"       -> "core/src/main/scala/mongo4s/operations/TransactionOptions.scala",
    "Update"                   -> "core/src/main/scala/mongo4s/operations/Update.scala",
    "UpdateOptions"            -> "core/src/main/scala/mongo4s/operations/UpdateOptions.scala",
    "VectorSimilarity"         -> "core/src/main/scala/mongo4s/operations/SearchIndex.scala",
    "WatchOptions"             -> "core/src/main/scala/mongo4s/changestream/WatchOptions.scala",
    "WindowFunction"           -> "core/src/main/scala/mongo4s/operations/Window.scala",
    "WireCodecConfig"          -> "bson/direct/src/main/scala/mongo4s/bson/direct/WireCodecConfig.scala",
    "WriteCommand"             -> "core/src/main/scala/mongo4s/operations/WriteCommand.scala",
  )

final class ReadmeReferencesSpec extends AnyWordSpec, Matchers:
  import ReadmeReferencesSpec.known

  private val fence     = "```scala"
  private val reference = raw"\b([A-Z][A-Za-z0-9]*)\.([a-z][A-Za-z0-9]*)".r

  private def codeBlocks(markdown: String): List[String] =
    markdown
      .split("\n")
      .foldLeft((List.empty[String], false, StringBuilder())) { case ((blocks, inside, current), line) =>
        if line.startsWith(fence) then (blocks, true, StringBuilder())
        else if inside && line.startsWith("```") then (blocks :+ current.toString, false, StringBuilder())
        else if inside then (blocks, true, current.append(line).append("\n"))
        else (blocks, inside, current)
      }
      ._1

  private val definition = raw"\b(?:inline\s+)?(?:def|val|case)\s+([a-zA-Z][A-Za-z0-9]*)".r

  private def members(source: String): Set[String] =
    val file = Path.of(source)

    if !Files.exists(file)
    then Set.empty
    else definition.findAllMatchIn(Files.readString(file)).map(_.group(1)).toSet

  "every mongo4s name the README's Scala blocks mention" should {

    "exist in the code" in {
      val markdown = Files.readString(Path.of("README.md"))
      val blocks   = codeBlocks(markdown)

      blocks should not be empty

      withClue("a mapped source is missing, which would make this check pass on nothing — ") {
        known.values.filterNot(source => Files.exists(Path.of(source))) shouldBe empty
      }

      val missing =
        for
          block    <- blocks
          m        <- reference.findAllMatchIn(block).toList
          source   <- known.get(m.group(1)).toList
          available = members(source)
          if !available.contains(m.group(2))
        yield s"${m.group(1)}.${m.group(2)}"

      withClue(s"named in README but not defined in the source that owns it: ${missing.distinct.mkString(", ")} — ") {
        missing.distinct shouldBe empty
      }
    }
  }
