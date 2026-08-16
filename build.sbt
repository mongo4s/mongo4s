import Dependencies.Versions

lazy val commonSettings = Seq(
  organization           := "org.mongo4s",
  organizationName       := "Mongo4s",
  homepage               := Some(uri("https://mongo4s.org/")),
  description            := "Mongo client, bson-codecs and repositories for Scala 3",
  version                := "0.2.0",
  versionScheme          := Some("semver-spec"),
  scalaVersion           := Versions.scalaLTS,
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
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-source:future",
    "-Wunused:all"
  ),
  credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials"),
)

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
    name         := "mongo4s-bson-calypso",
    scalaVersion := Versions.scalaLast,
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
    publish / skip := true
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
    name         := "mongo4s-kyo",
    scalaVersion := Versions.scalaLast,
    libraryDependencies ++= Dependencies.Kyo.all,
  )
  .dependsOn(core)

lazy val rapid = project
  .in(file("runtime/rapid"))
  .settings(commonSettings)
  .settings(
    name         := "mongo4s-rapid",
    scalaVersion := Versions.scalaLast,
    libraryDependencies ++= Dependencies.Rapid.all,
  )
  .dependsOn(core)

lazy val runtime = project
  .in(file("runtime"))
  .settings(commonSettings)
  .settings(
    publish / skip := true
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
  .dependsOn(core, cats % Test)

lazy val repositoriesTests = project
  .in(file("repositories-tests"))
  .settings(commonSettings)
  .settings(
    name           := "mongo4s-repositories-tests",
    scalaVersion   := Versions.scalaLast,
    publish / skip := true,
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
    repositories % "test->test;test->compile",
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
    name           := "mongo4s-examples",
    scalaVersion   := Versions.scalaLast,
    publish / skip := true,
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
    name           := "mongo4s-benchmarks",
    scalaVersion   := Versions.scalaLast,
    publish / skip := true,
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
    name           := "mongo4s-it",
    scalaVersion   := Versions.scalaLast,
    publish / skip := true,
    Test / fork    := true,
    Test / envVars ++= Map(
      "DOCKER_HOST" -> sys.env.getOrElse("DOCKER_HOST", "tcp://localhost:2375"),
    ),
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
  )

lazy val root = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name           := "mongo4s",
    publish / skip := true,
  )
  .aggregate(
    bson,
    core,
    runtime,
    repositories,
  )
