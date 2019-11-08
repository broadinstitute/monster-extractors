val betterFilesVersion = "3.8.0"
val circeVersion = "0.12.3"
val declineVersion = "1.0.0"
val fs2Version = "2.1.0"
val logbackVersion = "1.2.3"
val log4CatsVersion = "1.0.0"
val staxonVersion = "1.3"

// Testing.
val scalaTestVersion = "3.0.8"

lazy val `monster-extractors` = project
  .in(file("."))
  .aggregate(xml, `xml-clp`)
  .settings(publish / skip := true)

lazy val xml = project
  .in(file("xml"))
  .enablePlugins(MonsterBasePlugin)
  .settings(
    publish / skip := true,
    // Main code.
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "de.odysseus.staxon" % "staxon" % staxonVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

lazy val `xml-clp` = project
  .in(file("xml/clp"))
  .enablePlugins(MonsterDockerPlugin)
  .dependsOn(xml)
  .settings(
    libraryDependencies ++= Seq(
      "com.monovore" %% "decline" % declineVersion,
      "com.monovore" %% "decline-effect" % declineVersion
    )
  )
