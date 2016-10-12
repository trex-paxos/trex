
val mapdbVersion = "1.0.9"
val scalatestVersion = "2.2.5"
val scalmockVersion = "3.2.2"
val akkaVersion = "2.3.15"
val logbackVersion = "1.1.7"

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
		  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
		  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
		  "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test,it",
		  "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test,it",
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
		  )
  )
  
lazy val demo = project.dependsOn(core).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" % "akka-slf4j_2.11" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.11" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalmockVersion % "test"
    )
  )
