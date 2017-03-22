
val mapdbVersion = "1.0.9"
val scalatestVersion = "3.0.1"
val scalmockVersion = "3.3.0"
val akkaVersion = "2.3.15"
val disruptorVersion = "3.3.6"
val logbackVersion = "1.2.1"
val nettyVersion = "4.1.8.Final"
val metricsVersion = "3.2.1"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.8",
  organization := "com.github.trex-paxos",
  version := "0.3",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings")
)

lazy val root = (project in file(".")).aggregate(library,core,demo).settings(
  packagedArtifacts := Map.empty
)

lazy val library = project.settings(commonSettings: _*).
  settings( name := "trex-library").
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test"
    )
  )

lazy val core = project.dependsOn(library).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-core").
  settings(
		libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.2.1",
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
      "io.netty" % "netty-all" % nettyVersion,
		  "com.typesafe.akka" %% "akka-actor" % akkaVersion % "test,it",
		  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test,it",
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it"
  )
)

lazy val netty = project.dependsOn(core).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-netty").
  settings(
    libraryDependencies ++= Seq(
      "io.netty" % "netty-all" % nettyVersion,
      "io.dropwizard.metrics" % "metrics-core" % metricsVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it"
    )
  )

lazy val demo = project.dependsOn(netty).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" % "akka-slf4j_2.11" % akkaVersion,
      "com.lmax" % "disruptor" % disruptorVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test"
    )
  )
