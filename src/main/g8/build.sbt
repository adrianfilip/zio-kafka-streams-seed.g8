lazy val library =
  new {

    object Version {
      val zio       = "1.0.3"
    }

    val zio        = "dev.zio" %% "zio"          % Version.zio
    val zioTest    = "dev.zio" %% "zio-test"     % Version.zio
    val zioTestSbt = "dev.zio" %% "zio-test-sbt" % Version.zio

  }

lazy val root =
  project
    .in(file("."))
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        library.zio,
        library.zioTest    % Test,
        library.zioTestSbt % Test,
        "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
        "org.apache.kafka" % "kafka-streams-test-utils" % "2.6.0" % Test,
      ),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )

lazy val settings =
  commonSettings ++
    commandAliases

lazy val commonSettings =
  Seq(
    name := "zio-kafka-streams",
    scalaVersion := "2.13.3",
    organization := "com.example"
  )

lazy val commandAliases =
  addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt") ++
    addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

// *****************************************************************************
// Scalac
// *****************************************************************************

lazy val stdOptions = Seq(
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-Yrangepos",
  "-feature",
  "-language:higherKinds",
  "-language:existentials",
  "-Xlint:_,-type-parameter-shadow",
  "-Xsource:2.13",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings"
)

lazy val stdOpts213 = Seq(
  "-Wunused:imports",
  "-Wvalue-discard",
  "-Wunused:patvars",
  "-Wunused:privates",
  "-Wunused:params",
  "-Wvalue-discard"
)

scalacOptions := stdOptions ++ stdOpts213
