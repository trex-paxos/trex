//enablePlugins(ScalaNativePlugin)

val mapdbVersion = "1.0.9"
val scalatestVersion = "3.0.8"
val scalamockVersion = "3.6.0"
val akkaVersion = "2.5.14"
val logbackVersion = "1.2.3"
val argonautVersion = "6.2.2"

lazy val commonSettings = Seq(
  scalaVersion := "2.12.10",
  organization := "com.github.trex-paxos",
  version := "0.4",
  scalacOptions := Seq("-feature", "-deprecation", "-Xfatal-warnings")
)

lazy val root = (project in file(".")).aggregate(library,core,demo).settings(
  packagedArtifacts := Map.empty
)

lazy val library = project.settings(commonSettings: _*).
  settings( name := "trex-library").
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.12" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalamockVersion % "test"
    )
//    ,
//    nativeLinkStubs := true
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
      "io.argonaut" %% "argonaut" % argonautVersion,
      "org.scalatest" % "scalatest_2.12" % scalatestVersion % "test,it",
		  "org.scalamock" %% "scalamock-scalatest-support" % scalamockVersion % "test,it",
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
		  )
  )
  
lazy val demo = project.dependsOn(core).
	settings(commonSettings: _*).
	settings( name := "trex-demo").
  settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "com.typesafe.akka" % "akka-slf4j_2.12" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.mapdb" % "mapdb" % mapdbVersion,
      "org.scalatest" % "scalatest_2.12" % scalatestVersion % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % scalamockVersion % "test"
    )
  )
