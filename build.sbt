val betterFilesVersion = "3.8.0"
val declineVersion = "1.0.0"
val gcsLibVersion = "0.5.0"
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
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "de.odysseus.staxon" % "staxon" % staxonVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion,
      "org.broadinstitute.monster" %% "gcs-lib" % gcsLibVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
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
