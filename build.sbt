val mvstoreVersion = "2.0.204"
val akkaVersion = "2.5.32"
val logbackVersion = "1.2.10"
val argonautVersion = "6.3.7"
val scalatestVersion = "3.2.10"
val scalamockVersion = "5.2.0"
val liftjsonVersion = "3.5.0"
lazy val scala2 = "2.13.7"

Global / onChangedBuildSource := ReloadOnSourceChanges
publish / skip := true

lazy val commonSettings = Seq(
  scalaVersion := scala2,
  organization := "com.github.trex-paxos",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings"),
)

lazy val root = (project in file(".")).aggregate(library,core,demo).settings(
  packagedArtifacts := Map.empty
)

// TODO no mocking/stubing framework seems to support scala3 yet
lazy val library = project.settings(commonSettings: _*).
  settings( name := "trex-library").
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalamock" %% "scalamock" % scalamockVersion % Test
    )
  )

// TODO move akka to a test dependency to use its async test kit for testing
lazy val core = project.dependsOn(library).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-core").
  settings(
    Test / parallelExecution := false,
    libraryDependencies ++= Seq(
      "com.h2database" % "h2-mvstore" % mvstoreVersion,
      "io.argonaut" %% "argonaut" % argonautVersion,
      "com.typesafe" % "config" % "1.3.3",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,it",
      "org.scalamock" %% "scalamock" % scalamockVersion % Test,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
    )
  )

// TODO replace akka with rsocket
lazy val demo = project.dependsOn(core).
  settings(commonSettings: _*).
  settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "net.liftweb" %% "lift-json" % liftjsonVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor-typed" %akkaVersion,
      "com.h2database" % "h2-mvstore" % mvstoreVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalamock" %% "scalamock" % scalamockVersion % Test
    )
  )
