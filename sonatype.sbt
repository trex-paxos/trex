// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.github.trex-paxos"

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>http://trex-paxos.github.io/trex/</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/trex-paxos/trex.git</connection>
      <developerConnection>scm:git:git@github.com:trex-paxos/trex.git</developerConnection>
      <url>https://github.com/trex-paxos/trex</url>
    </scm>
    <developers>
      <developer>
        <id>simbo1905</id>
        <name>Simon Massey</name>
        <url>https://simbo1905.wordpress.com</url>
      </developer>
    </developers>
}