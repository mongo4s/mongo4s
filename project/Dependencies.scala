import sbt.*

object Dependencies {

  object Versions {
    val scalaLTS  = "3.3.8"
    val scalaLast = "3.8.4"

    val mongo = "5.9.1"

    val cats               = "2.13.0"
    val catsEffect3        = "3.7.0"
    val catsEffectTesting  = "1.8.0"
    val fs2                = "3.13.0"

    val zio                       = "2.1.26"
    val zioPrelude                = "1.0.0-RC48"
    val zioInteropReactiveStreams = "2.0.2"

    val kyo = "1.0.0-RC6"

    val rapid = "2.9.9"

    val medeia  = "1.0.9"
    val zioBson = "1.0.11"
    val calypso = "0.7.0"

    val mongo4cats  = "0.7.18"
    val circe       = "0.14.10"
    val zioSchema   = "1.8.5"

    val testcontainers = "1.21.4"

    val scalatest = "3.2.20"
  }

  object Benchmarks {
    val mongo4catsCore    = "io.github.kirill5k" %% "mongo4cats-core"     % Versions.mongo4cats
    val mongo4catsCirce   = "io.github.kirill5k" %% "mongo4cats-circe"    % Versions.mongo4cats
    val mongo4catsZio     = "io.github.kirill5k" %% "mongo4cats-zio"      % Versions.mongo4cats
    val mongo4catsZioJson = "io.github.kirill5k" %% "mongo4cats-zio-json" % Versions.mongo4cats
    val circeGeneric      = "io.circe"           %% "circe-generic"       % Versions.circe
    val zioSchema         = "dev.zio"            %% "zio-schema"          % Versions.zioSchema
    val zioSchemaDerivation = "dev.zio"          %% "zio-schema-derivation" % Versions.zioSchema
    val zioSchemaBson     = "dev.zio"            %% "zio-schema-bson"     % Versions.zioSchema
  }

  object IntegrationTesting {
    val testcontainersMongo = "org.testcontainers" % "mongodb" % Versions.testcontainers % Test
  }

  object Codecs {
    val medeia  = "de.megaera" %% "medeia"       % Versions.medeia
    val zioBson = "dev.zio"    %% "zio-bson"     % Versions.zioBson
    val calypso = "ru.m2"      %% "calypso-core" % Versions.calypso
  }

  object Mongo {
    val bson           = "org.mongodb" % "bson"                            % Versions.mongo
    val driverCore     = "org.mongodb" % "mongodb-driver-core"             % Versions.mongo
    val driverReactive = "org.mongodb" % "mongodb-driver-reactivestreams"  % Versions.mongo
    val driverSync     = "org.mongodb" % "mongodb-driver-sync"             % Versions.mongo
  }

  object Cats {
    val catsCore = "org.typelevel" %% "cats-core" % Versions.cats

    val catsEffect3        = "org.typelevel" %% "cats-effect"                   % Versions.catsEffect3
    val catsEffect3Kernel  = "org.typelevel" %% "cats-effect-kernel"            % Versions.catsEffect3
    val catsEffect3Testing = "org.typelevel" %% "cats-effect-testing-scalatest" % Versions.catsEffectTesting % Test

    val fs2Core            = "co.fs2" %% "fs2-core"             % Versions.fs2
    val fs2ReactiveStreams = "co.fs2" %% "fs2-reactive-streams" % Versions.fs2

    val all: Seq[ModuleID] = Seq(catsEffect3Kernel, fs2Core, fs2ReactiveStreams, catsEffect3 % Test, catsEffect3Testing)
  }

  object Zio {
    val zio                       = "dev.zio" %% "zio"                         % Versions.zio
    val zioStreams                = "dev.zio" %% "zio-streams"                 % Versions.zio
    val zioPrelude                = "dev.zio" %% "zio-prelude"                 % Versions.zioPrelude
    val zioInteropReactiveStreams = "dev.zio" %% "zio-interop-reactivestreams" % Versions.zioInteropReactiveStreams

    val all: Seq[ModuleID] = Seq(zio, zioStreams, zioInteropReactiveStreams)
  }

  object Kyo {
    val kyoCore            = "io.getkyo" %% "kyo-core"             % Versions.kyo
    val kyoData            = "io.getkyo" %% "kyo-data"              % Versions.kyo
    val kyoPrelude         = "io.getkyo" %% "kyo-prelude"           % Versions.kyo
    val kyoReactiveStreams = "io.getkyo" %% "kyo-reactive-streams"  % Versions.kyo

    val all: Seq[ModuleID] = Seq(kyoCore, kyoData, kyoPrelude, kyoReactiveStreams)
  }

  object Rapid {
    val rapidCore = "com.outr" %% "rapid-core" % Versions.rapid
    val rapidTest = "com.outr" %% "rapid-test" % Versions.rapid % Test

    val all: Seq[ModuleID] = Seq(rapidCore, rapidTest)
  }

  object Testing {
    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test
    val scalactic = "org.scalactic" %% "scalactic" % Versions.scalatest % Test

    val all: Seq[ModuleID] = Seq(scalatest, scalactic)
  }

}
