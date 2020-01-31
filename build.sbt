val betterFilesVersion = "3.8.0"
val catsEffectVersion = "2.1.0"
val circeVersion = "0.12.3"
val declineVersion = "1.0.0"
val fs2Version = "2.2.2"
val jettisonVersion = "1.4.0"
val logbackVersion = "1.2.3"
val log4CatsVersion = "1.0.0"
val woodstoxVersion = "6.0.2"

// Testing.
val scalaTestVersion = "3.0.8"

lazy val `monster-xml-to-json-list` = project
  .in(file("."))
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
