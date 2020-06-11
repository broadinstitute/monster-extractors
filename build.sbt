import _root_.io.circe.Json

val betterFilesVersion = "3.8.0"
val catsEffectVersion = "2.1.2"
val circeVersion = "0.13.0"
val declineVersion = "1.2.0"
val fs2Version = "2.3.0"
val jettisonVersion = "1.4.1"
val logbackVersion = "1.2.3"
val log4CatsVersion = "1.0.1"
val woodstoxVersion = "6.1.1"

// Testing.
val scalaTestVersion = "3.1.1"

lazy val `monster-xml-to-json-list` = project
  .in(file("."))
  .aggregate(`xml-to-json-list-clp`, `xml-to-json-list-template`)
  .settings(publish / skip := true)

lazy val `xml-to-json-list-clp` = project
  .in(file("clp"))
  .enablePlugins(MonsterDockerPlugin)
  .settings(
    // Main code.
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "com.fasterxml.woodstox" % "woodstox-core" % woodstoxVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion,
      "org.codehaus.jettison" % "jettison" % jettisonVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "com.monovore" %% "decline" % declineVersion,
      "com.monovore" %% "decline-effect" % declineVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

  lazy val `xml-to-json-list-template` = project
    .in(file("argo"))
    .enablePlugins(MonsterHelmPlugin)
    .settings(
      helmChartOrganization := "broadinstitute",
      helmChartRepository := "monster-xml-to-json-list",
      helmInjectVersionValues := { (baseValues, version) =>
        baseValues.deepMerge(Json.obj("version" -> Json.fromString(version)))
      }
    )
