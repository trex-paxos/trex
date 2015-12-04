
val mapdbVersion = "1.0.8"
val scalatestVersion = "2.2.5"
val scalmockVersion = "3.2.2"
val akkaVersion = "2.3.14"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.7",
  organization := "com.github.trex-paxos",
  version := "0.1",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings")
)

lazy val root = (project in file(".")).aggregate(library,core,demo)

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
		  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
		  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
		  "org.mapdb" % "mapdb" % mapdbVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
		  "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
		  "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it"
		  )
  )
  
lazy val demo = project.dependsOn(core).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  //settings(mainClass in (Compile, run) := Some("com.github.simbo1905.trexdemo.TrexKVStore")).
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test"
    )
  )
