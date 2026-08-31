import com.typesafe.tools.mima.core.{DirectMissingMethodProblem, MissingTypesProblem, ProblemFilters}
import com.typesafe.tools.mima.plugin.MimaKeys.mimaBinaryIssueFilters

lazy val wireCodecConfigNoLongerACaseClass = Seq(
  ProblemFilters.exclude[MissingTypesProblem]("mongo4s.bson.direct.WireCodecConfig"),
  ProblemFilters.exclude[MissingTypesProblem]("mongo4s.bson.direct.WireCodecConfig$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.this"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.unapply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.fromProduct"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.canEqual"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.productArity"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.productPrefix"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.productElement"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.productElementName"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig._1"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig._2"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig._3"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.<init>$default$1"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.<init>$default$2"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.<init>$default$3"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.copy$default$1"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.copy$default$2"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("mongo4s.bson.direct.WireCodecConfig.copy$default$3"),
)

LocalProject("bsonDirect") / mimaBinaryIssueFilters ++= wireCodecConfigNoLongerACaseClass
