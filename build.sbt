
val mapdbVersion = "1.0.9"
val scalatestVersion = "3.0.1"
val scalmockVersion = "3.3.0"
val akkaVersion = "2.3.15"
val log4j2Version = "2.7"
val disruptorVersion = "3.3.6"

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

lazy val coreng = project.dependsOn(library).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-coreng").
  settings(
    libraryDependencies ++= Seq(
      "org.mapdb" % "mapdb" % mapdbVersion,
      "io.netty" % "netty-all" % "4.1.6.Final",
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it"
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
		  "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
      "io.netty" % "netty-all" % "4.1.6.Final",
		  "com.typesafe.akka" %% "akka-actor" % akkaVersion % "test,it",
		  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test,it",
"org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
"org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it"
)
)
  
lazy val demo = project.dependsOn(core).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" % "akka-slf4j_2.11" % akkaVersion,
      //"ch.qos.logback" % "logback-classic" % logbackVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4j2Version,
      "org.apache.logging.log4j" % "log4j-core" % log4j2Version,
      "com.lmax" % "disruptor" % disruptorVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test"
    )
  )
