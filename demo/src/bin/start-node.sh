#!/bin/bash
# first run `sbt assembly`
export SCALA_VERSION=2.13
export TREX_VERSION=0.5.2-SNAPSHOT
java -cp ./demo/src/main/resources:./demo/target/scala-${SCALA_VERSION}/trex-demo-assembly-${TREX_VERSION}.jar com.github.trex_paxos.demo.TrexKVStore $*
