
lazy val commonSettings = Seq(
  scalaVersion := "2.11.7",
  organization := "com.github.simbo1905",
  version := "0.1",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings")
)

lazy val root = (project in file(".")).aggregate(library,core,demo)

lazy val library = project.settings(commonSettings: _*).
  settings( name := "trex-library").
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"
    )
  )


lazy val core = project.dependsOn(library).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(name := "trex-core").
  settings(
		libraryDependencies ++= Seq(
		  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
		  "com.typesafe.akka" %% "akka-testkit" % "2.3.9",
		  "org.mapdb" % "mapdb" % "1.0.6",
		  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test,it",
		  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test,it"
		  )
  )
  
lazy val demo = project.dependsOn(core).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  //settings(mainClass in (Compile, run) := Some("com.github.simbo1905.trexdemo.TrexKVStore")).
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" %% "akka-actor" % "2.3.9",
      "com.typesafe.akka" %% "akka-remote" % "2.3.9",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.9",
      "org.mapdb" % "mapdb" % "1.0.6",
      "org.scala-lang.modules" %% "scala-pickling" % "0.10.0",
      "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"
    )
  )
