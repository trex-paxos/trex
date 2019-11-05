ThisBuild / organization := "com.github.trex-paxos"
ThisBuild / organizationName := "trex"
ThisBuild / organizationHomepage := Some(url("https://github.com/trex-paxos/trex"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/trex-paxos/trex"),
    "scm:git:github.com/trex-paxos/trex.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "simbo1905",
    name  = "Simon Massey",
    email = "simbo1905@60hertz.com",
    url   = url("http://simbo1905.blog")
  )
)

ThisBuild / description := "Embeddable multi-Paxos For The JVM"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/trex-paxos/trex"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
