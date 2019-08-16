// Settings to apply across the entire build
inThisBuild(
  Seq(
    organization := "org.broadinstitute.monster",
    scalaVersion := "2.12.8",
    // Auto-format
    scalafmtConfig := (ThisBuild / baseDirectory)(_ / ".scalafmt.conf").value,
    scalafmtOnCompile := true,
    // Recommended guardrails
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xcheckinit",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Xmax-classfile-name",
      "200",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    )
  )
)

// Compiler plugins.
val betterMonadicForVersion = "0.3.1"

val betterFilesVersion = "3.8.0"
val gcsLibVersion = "0.1.0"
val staxonVersion = "1.3"

// Testing.
val scalaTestVersion = "3.0.8"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(
    Set(
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-unused",
      "-Ywarn-unused-import"
    )
  ),

  resolvers ++= Seq(
    "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/",
    "Broad Artifactory Snapshots" at "https://broadinstitute.jfrog.io/broadinstitute/libs-snapshot/"
  ),

  Compile / doc / scalacOptions += "-no-link-warnings",
  Test / fork := true
)

lazy val `monster-extractors` = project
  .in(file("."))
  .aggregate(xml)
  .settings(publish / skip := true)

lazy val xml = project
  .in(file("xml"))
  .configs(IntegrationTest)
  .enablePlugins(PublishPlugin)
  .settings(commonSettings)
  .settings(
    Defaults.itSettings,
    // Main code.
    libraryDependencies ++= Seq(
      "org.broadinstitute.monster" %% "gcs-lib" % gcsLibVersion,
      "de.odysseus.staxon" % "staxon" % staxonVersion,
      "com.github.pathikrit" %% "better-files" % betterFilesVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}"),
  )
