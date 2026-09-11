import Dependencies.Versions
import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

lazy val binaryCompatibleWith = Set.empty[String]

// Every module is built and published for one JDK. `mongo4s-kyo` forces the floor: kyo's `Frame` macro runs inside
// the compiler and its class files target Java 25, so anything older cannot compile that module at all. Rather than
// let four modules build and the fifth fail with `UnsupportedClassVersionError: class file version 69.0`, the build
// refuses up front and says why.
lazy val requiredJdk = 25

lazy val checkedJdk: Int = {
  val running = sys.props.getOrElse("java.specification.version", "unknown")
  val major   = running.split('.').headOption.flatMap(_.toIntOption).getOrElse(0)

  if major < requiredJdk then
    sys.error(
      s"mongo4s requires JDK $requiredJdk or newer; this build is running on JDK $running. " +
        "mongo4s-kyo's class files target Java 25 and cannot be compiled by an older JDK. " +
        "Point JAVA_HOME at a JDK 25 install (`cs java --jvm 25`, or `brew install openjdk@25`) and retry."
    )

  major
}

// Pinned rather than `future`, because on 3.9 `-source:future` means 3.10 semantics — a moving target under a
// published library. CI overrides it in an advisory job
// (`sbt 'set every sourceLevel := "future"; Test/compile'`), so a break in the next Scala release shows up there
// instead of in the build everyone depends on.
lazy val sourceLevel = settingKey[String]("Scala -source level the build compiles against")

lazy val commonSettings = { val _ = checkedJdk; Seq(
  organization           := "org.mongo4s",
  organizationName       := "Mongo4s",
  homepage               := Some(uri("https://mongo4s.org/")),
  description            := "Mongo client, bson-codecs and repositories for Scala 3",
  version                := "3.0.0",
  versionScheme          := Some("semver-spec"),
  scalaVersion           := Versions.scala3,
  parallelExecution      := true,
  publishMavenStyle      := true,
  Test / publishArtifact := false,
  licenses               := List(License.Apache2),
  pomIncludeRepository   := { _ => false },
  publishTo              := localStaging.value,
  scmInfo                := Some(
    ScmInfo(
      uri("https://github.com/mongo4s/mongo4s"),
      "git@github.com:mongo4s/mongo4s.git",
    )
  ),
  developers             := List(
    Developer(
      "shadowsmind",
      "Alexandr Oshlakov",
      "shadowsmind.dev@gmail.com",
      uri("https://github.com/shadowsmind"),
    )
  ),
  libraryDependencies ++= Dependencies.Testing.all,
  exportJars             := false,
  sourceLevel            := "3.9",
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    s"-source:${sourceLevel.value}",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xcheck-macros",
    "-Wunused:all",
    "-Wvalue-discard",
    "-WunstableInlineAccessors",
    "-Wshadow:all",
    "-Winfer-union",
    "-Wimplausible-patterns",
    "-Wrecurse-with-default",
    // Not `-Wsafe-init`: its analysis crashes kyo's Tag macro with a MatchError on an HKTypeLambda. The same call
    // sites already draw "Missing symbol position ... This is a compiler bug" under the plain flags, so the fault is
    // upstream rather than here. Not `-Wnonunit-statement` either — every non-final ScalaTest assertion trips it.
  ),
  credentials ++= Seq(Path.userHome / ".sbt" / "sonatype_credentials").filter(_.isFile).map(Credentials(_)),
  mimaPreviousArtifacts  := binaryCompatibleWith.map(organization.value %% moduleName.value % _),
) }

lazy val bsonCore = project
  .in(file("bson/core"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-core",
    libraryDependencies += Dependencies.Mongo.bson,
  )

lazy val bsonMedeia = project
  .in(file("bson/medeia"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-medeia",
    libraryDependencies += Dependencies.Codecs.medeia,
  )
  .dependsOn(bsonCore)

lazy val bsonZio = project
  .in(file("bson/zio"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-zio",
    libraryDependencies += Dependencies.Codecs.zioBson,
  )
  .dependsOn(bsonCore)

lazy val bsonCalypso = project
  .in(file("bson/calypso"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-calypso",
    libraryDependencies += Dependencies.Codecs.calypso,
  )
  .dependsOn(bsonCore)

lazy val bsonDirect = project
  .in(file("bson/direct"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-direct"
  )
  .dependsOn(bsonCore)

lazy val bsonCatsData = project
  .in(file("bson/cats-data"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-bson-cats-data",
    libraryDependencies += Dependencies.Cats.catsCore,
  )
  .dependsOn(bsonCore, bsonDirect)

lazy val bson = project
  .in(file("bson"))
  .settings(commonSettings)
  .settings(
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
  )
  .aggregate(
    bsonCore,
    bsonMedeia,
    bsonZio,
    bsonCalypso,
    bsonDirect,
    bsonCatsData,
  )

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-core",
    libraryDependencies ++= Seq(
      Dependencies.Mongo.driverCore,
      Dependencies.Mongo.driverReactive,
    ),
  )
  .dependsOn(bsonCore, bsonDirect)

lazy val cats = project
  .in(file("runtime/cats"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-cats",
    libraryDependencies ++= Dependencies.Cats.all,
  )
  .dependsOn(core)

lazy val zio = project
  .in(file("runtime/zio"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-zio",
    libraryDependencies ++= Dependencies.Zio.all,
  )
  .dependsOn(core)

lazy val kyo = project
  .in(file("runtime/kyo"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-kyo",
    libraryDependencies ++= Dependencies.Kyo.all,
  )
  .dependsOn(core)

lazy val rapid = project
  .in(file("runtime/rapid"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-rapid",
    libraryDependencies ++= Dependencies.Rapid.all,
  )
  .dependsOn(core)

lazy val runtime = project
  .in(file("runtime"))
  .settings(commonSettings)
  .settings(
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
  )
  .aggregate(
    cats,
    zio,
    kyo,
    rapid,
  )

lazy val repositories = project
  .in(file("repositories"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-repositories",
    libraryDependencies ++= Seq(Dependencies.Cats.catsEffect3 % Test, Dependencies.Cats.catsEffect3Testing),
  )
  .dependsOn(core)

lazy val testkit = project
  .in(file("testkit"))
  .settings(commonSettings)
  .settings(
    name := "mongo4s-testkit",
  )
  .dependsOn(core, repositories)

lazy val repositoriesTests = project
  .in(file("repositories-tests"))
  .settings(commonSettings)
  .settings(
    name                  := "mongo4s-repositories-tests",
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
    libraryDependencies ++= Seq(
      Dependencies.Cats.catsEffect3,
      Dependencies.Cats.catsEffect3Testing,
      Dependencies.Codecs.medeia,
      Dependencies.Codecs.zioBson,
      Dependencies.Codecs.calypso,
      Dependencies.Benchmarks.zioSchema,
      Dependencies.Benchmarks.zioSchemaDerivation,
      Dependencies.Benchmarks.zioSchemaBson,
    ),
  )
  .dependsOn(
    repositories,
    testkit,
    cats,
    zio,
    rapid,
    kyo,
    bsonMedeia,
    bsonZio,
    bsonCalypso,
  )

lazy val examples = project
  .in(file("examples"))
  .settings(commonSettings)
  .settings(
    name                  := "mongo4s-examples",
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
    libraryDependencies ++= Seq(
      Dependencies.Cats.catsEffect3,
      Dependencies.Codecs.medeia,
      Dependencies.Codecs.zioBson,
      Dependencies.Codecs.calypso,
      Dependencies.Benchmarks.zioSchema,
      Dependencies.Benchmarks.zioSchemaDerivation,
      Dependencies.Benchmarks.zioSchemaBson,
    ),
  )
  .dependsOn(
    cats,
    zio,
    kyo,
    rapid,
    repositories,
    bsonMedeia,
    bsonZio,
    bsonCalypso,
    bsonDirect,
  )

lazy val benchmarks = project
  .in(file("benchmarks"))
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    name                  := "mongo4s-benchmarks",
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
    libraryDependencies ++= Seq(
      Dependencies.Benchmarks.mongo4catsCore,
      Dependencies.Benchmarks.mongo4catsCirce,
      Dependencies.Benchmarks.mongo4catsZio,
      Dependencies.Benchmarks.mongo4catsZioJson,
      Dependencies.Benchmarks.circeGeneric,
      Dependencies.Benchmarks.zioSchema,
      Dependencies.Benchmarks.zioSchemaDerivation,
      Dependencies.Benchmarks.zioSchemaBson,
      Dependencies.Cats.catsEffect3,
    ),
  )
  .dependsOn(
    bsonMedeia,
    bsonZio,
    bsonCalypso,
    cats,
    zio,
    kyo,
    rapid,
  )

lazy val it = project
  .in(file("it"))
  .settings(commonSettings)
  .settings(
    name                     := "mongo4s-it",
    publish / skip           := true,
    mimaPreviousArtifacts    := Set.empty,
    Test / fork              := true,
    Test / parallelExecution := false,
    Test / envVars ++= sys.env
      .get("DOCKER_HOST")
      .map("DOCKER_HOST" -> _)
      .orElse(Option.when(scala.util.Properties.isWin)("DOCKER_HOST" -> "tcp://localhost:2375"))
      .toMap,
    libraryDependencies ++= Seq(
      Dependencies.Cats.catsEffect3,
      Dependencies.Cats.catsEffect3Testing,
      Dependencies.IntegrationTesting.testcontainersMongo,
    ),
  )
  .dependsOn(
    core,
    cats,
    zio,
    kyo,
    rapid,
    bsonMedeia,
    repositories,
    testkit,
  )

lazy val root = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name                  := "mongo4s",
    publish / skip        := true,
    mimaPreviousArtifacts := Set.empty,
  )
  .aggregate(
    bson,
    core,
    runtime,
    testkit,
    repositories,
    repositoriesTests,
    examples,
    benchmarks,
  )
