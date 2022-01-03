val mapdbVersion = "1.0.9"
val scalatestVersion = "3.2.10"
val akkaVersion = "2.5.32"
val logbackVersion = "1.2.10"
val argonautVersion = "6.3.7"

releaseIgnoreUntrackedFiles := true
//releasePublishArtifactsAction := PgpKeys.publishSigned.value

lazy val commonSettings = Seq(
  scalaVersion := "2.13.7",
  organization := "com.github.trex-paxos",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings")
)

lazy val root = (project in file(".")).aggregate(library,core,demo).settings(
  packagedArtifacts := Map.empty
)

lazy val library = project.settings(commonSettings: _*).
  settings( name := "trex-library").
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.13" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock" % "5.2.0" % Test
    )
  )

lazy val core = project.dependsOn(library).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-core").
  settings(
    Test / parallelExecution := false,
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.3",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "org.scalatest" % "scalatest_2.13" % scalatestVersion % "test,it",
      "org.scalamock" %% "scalamock" % "5.2.0" % Test,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
    )
  )

lazy val demo = project.dependsOn(core).
  settings(commonSettings: _*).
  settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor-typed" %akkaVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.13" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock" % "5.2.0" % Test
    )
  )
